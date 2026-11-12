FROM astrocrpublic.azurecr.io/runtime:3.3-12

# ---- Image metadata (OCI labels) ----
LABEL org.opencontainers.image.title="currency-etl-airflow" \
      org.opencontainers.image.description="Airflow (Astro Runtime) ETL pipeline: Frankfurter API exchange rates -> MongoDB" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.authors="Umer529" \
      maintainer="Umer529" \
      project="Task 3 - Building Airflow ETL Pipeline"

# Airflow connection to the MongoDB service (local development credentials only)
ENV AIRFLOW_CONN_MONGO_DEFAULT='{"conn_type": "mongo", "host": "mongodb", "port": 27017, "login": "admin", "password": "admin123", "schema": "currency_db", "extra": {"authSource": "admin"}}'
