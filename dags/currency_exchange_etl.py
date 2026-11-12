"""
Currency Exchange Rate ETL Pipeline
===================================

Daily pipeline that:
  1. Extracts the latest currency exchange rates from the Frankfurter API
  2. Transforms each rate into a flat, documented record
  3. Loads the records into MongoDB (upsert, so re-runs are idempotent)

Data moves between tasks using **named XComs** (explicit `key=` on push/pull).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import requests
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.sdk import dag, task

log = logging.getLogger(__name__)

API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY", "PKR", "CHF", "AUD", "CAD", "CNY"]
SOURCE_NAME = "Frankfurter API"

MONGO_CONN_ID = "mongo_default"
MONGO_DB = "currency_db"
MONGO_COLLECTION = "exchange_rates"

# Named XCom keys
XCOM_RAW_RATES = "raw_exchange_rates"
XCOM_TRANSFORMED_RATES = "transformed_exchange_rates"


@dag(
    dag_id="currency_exchange_etl",
    description="Daily EUR exchange rates: Frankfurter API -> transform -> MongoDB",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():

    @task(task_id="extract_exchange_rates")
    def extract_exchange_rates(ti=None):
        """Call the Frankfurter API and push the raw JSON response to XCom."""
        params = {"base": BASE_CURRENCY, "quotes": ",".join(TARGET_CURRENCIES)}
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        raw_rates = response.json()

        if not raw_rates:
            raise ValueError("Frankfurter API returned no exchange rates")

        payload = {
            "rates": raw_rates,
            "retrieved_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        }
        log.info("Extracted %d rates for base %s", len(raw_rates), BASE_CURRENCY)
        ti.xcom_push(key=XCOM_RAW_RATES, value=payload)

    @task(task_id="transform_exchange_rates")
    def transform_exchange_rates(ti=None):
        """Pull the raw rates, reshape them into the target schema and push the result."""
        payload = ti.xcom_pull(task_ids="extract_exchange_rates", key=XCOM_RAW_RATES)
        if not payload:
            raise ValueError(f"No data found in XCom key '{XCOM_RAW_RATES}'")

        transformed = []
        for item in payload["rates"]:
            if item.get("rate") is None:
                log.warning("Skipping record without rate: %s", item)
                continue
            transformed.append(
                {
                    "base_currency": item["base"].upper(),
                    "target_currency": item["quote"].upper(),
                    "exchange_rate": round(float(item["rate"]), 6),
                    "rate_date": item["date"],
                    "source": SOURCE_NAME,
                    "retrieved_at": payload["retrieved_at"],
                }
            )

        log.info("Transformed %d records", len(transformed))
        for record in transformed:
            log.info("%s", record)
        ti.xcom_push(key=XCOM_TRANSFORMED_RATES, value=transformed)

    @task(task_id="load_to_mongodb")
    def load_to_mongodb(ti=None):
        """Pull the transformed records and upsert them into MongoDB."""
        records = ti.xcom_pull(task_ids="transform_exchange_rates", key=XCOM_TRANSFORMED_RATES)
        if not records:
            raise ValueError(f"No data found in XCom key '{XCOM_TRANSFORMED_RATES}'")

        client = MongoHook(mongo_conn_id=MONGO_CONN_ID).get_conn()
        collection = client[MONGO_DB][MONGO_COLLECTION]
        collection.create_index(
            [("base_currency", 1), ("target_currency", 1), ("rate_date", 1)],
            unique=True,
            name="uniq_pair_per_day",
        )

        inserted = updated = 0
        for record in records:
            key = {k: record[k] for k in ("base_currency", "target_currency", "rate_date")}
            result = collection.update_one(key, {"$set": record}, upsert=True)
            if result.upserted_id is not None:
                inserted += 1
            else:
                updated += 1

        total = collection.count_documents({})
        log.info("MongoDB load done: %d inserted, %d updated, %d total documents", inserted, updated, total)

    extract_exchange_rates() >> transform_exchange_rates() >> load_to_mongodb()


currency_exchange_etl()
