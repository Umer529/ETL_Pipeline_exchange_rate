# Currency Exchange Rate ETL Pipeline (Airflow + Astro CLI + MongoDB)

Daily Airflow pipeline that pulls currency exchange rates from the
[Frankfurter API](https://frankfurter.dev/), transforms them, and loads them into MongoDB.
Mongo Express provides a web UI for the database.

```
extract_exchange_rates  ──►  transform_exchange_rates  ──►  load_to_mongodb
   (Frankfurter API)          (reshape to schema)            (upsert into MongoDB)
        │  XCom key: raw_exchange_rates   │  XCom key: transformed_exchange_rates
        └─────────────────────────────────┘
```

## Project structure

| Path | Purpose |
|------|---------|
| `dags/currency_exchange_etl.py` | The DAG with its 3 tasks |
| `Dockerfile` | Astro Runtime image + metadata `LABEL`s + Mongo Airflow connection |
| `docker-compose.override.yml` | MongoDB & Mongo Express services, custom network, persistent volumes |
| `requirements.txt` | `apache-airflow-providers-mongo`, `requests` |

## Tasks

1. **extract_exchange_rates** – calls `https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY,...`
   and pushes the raw response to XCom key `raw_exchange_rates`.
2. **transform_exchange_rates** – pulls `raw_exchange_rates`, produces one record per currency pair and
   pushes the list to XCom key `transformed_exchange_rates`:
   ```json
   {
     "base_currency": "EUR",
     "target_currency": "USD",
     "exchange_rate": 1.1117,
     "rate_date": "2026-11-12",
     "source": "Frankfurter API",
     "retrieved_at": "2026-11-12T18:46:15"
   }
   ```
3. **load_to_mongodb** – pulls `transformed_exchange_rates` and upserts into `currency_db.exchange_rates`
   (unique index on `base_currency + target_currency + rate_date`, so re-runs never create duplicates).

All data passes between tasks through **named XComs** (`ti.xcom_push(key=...)` / `ti.xcom_pull(task_ids=..., key=...)`).

## Docker setup

- **Metadata** – OCI labels (`title`, `description`, `version`, `authors`, `maintainer`, `project`) on the
  Airflow image (`Dockerfile`) and on the MongoDB / Mongo Express containers (`docker-compose.override.yml`).
  Check with `docker inspect <container> --format '{{json .Config.Labels}}'`.
- **Networking** – a user-defined bridge network `currency_etl_network` connects MongoDB, Mongo Express and the
  Airflow scheduler / dag-processor / api-server / triggerer, so tasks reach the database at host `mongodb`.
- **Persistent storage** – named volumes `currency_etl_mongo_data` (`/data/db`) and `currency_etl_mongo_config`
  keep the data across `astro dev stop` / `astro dev restart`.

## Running

```bash
astro dev start
```

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | – |
| Mongo Express | http://localhost:8081 | admin / admin123 |
| MongoDB | mongodb://localhost:27017 | admin / admin123 (authSource=admin) |

Unpause and trigger `currency_exchange_etl` in the Airflow UI, then open Mongo Express →
`currency_db` → `exchange_rates`.

> **Windows note:** if the project path contains `&` (e.g. `C:\Users\A & B\...`), Astro fails with
> `mkdir ...&amp;...: Access is denied`. Map the folder to a drive letter first:
> `subst P: "C:\path\to\ETL-Pipeline"` and run `astro dev start` from `P:\`.

Credentials here are for local development only.
