# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Energy Pipeline** is a modern data engineering project implementing a medallion architecture (bronze/silver/gold layers) to ingest weather and energy demand data into Azure Data Lake Storage, process and validate it, and visualize it through dashboards.

### Architecture

```
Data Sources (ENTSOE-E API, Open-Meteo API)
         ↓
Kafka Producers (weather_producer, energy_producer)
         ↓
Kafka Topics (raw.weather.hourly, raw.energy.demand, dead.letter.queue)
         ↓
Kafka Consumers
         ↓
Azure ADLS (Medallion Architecture)
  ├─ BRONZE (raw ingested data)
  │  └─ weather/year=2025/month=01/day=15/*.json (NDJSON)
  │  └─ energy/year=2025/month=01/day=15/*.json (NDJSON)
  │
  ├─ SILVER (cleaned, validated, enriched)
  │  └─ (PySpark jobs read from Bronze, write here)
  │
  └─ GOLD (business-ready, aggregated)
     └─ (dbt models or final transforms)
         ↓
PostgreSQL (via dbt / SQL transforms)
         ↓
Apache Superset (dashboards)
```

### Key Technologies

- **Orchestration**: Apache Airflow 2.8.1 (LocalExecutor)
- **Message Queue**: Kafka 7.5.0 + Zookeeper (coordination)
- **Database**: PostgreSQL 15 (Airflow metadata + data warehouse)
- **Transform**: dbt (staging → intermediate → marts)
- **Visualization**: Apache Superset 3.1.0
- **Cloud**: Azure Blob Storage, provisioned via Terraform
- **Languages**: Python 3.8+, SQL, HCL (Terraform)

### Directory Structure

- `ingestion/` - Kafka producers and consumers that write to Azure ADLS Bronze layer
  - `producers/` - Fetch raw data from APIs → publish to Kafka topics
    - `weather_producer.py` - Pulls from Open-Meteo API (historical data, 5+ days old)
    - `weather_backfill.py` - Backfill mode for historical data
    - `energy_producer.py` - Pulls from ENTSOE-E API
  - `consumers/` - Read from Kafka → write to Azure ADLS Bronze layer (NDJSON format, date-partitioned)
    - `weather_consumer.py` - Consumes raw.weather.hourly → bronze/weather/year=*/month=*/day=*/*.json
    - `energy_consumer.py` - Consumes raw.energy.demand → bronze/energy/year=*/month=*/day=*/*.json
  - `config.py` - Constants, API endpoints, city/country configs
- `processing/` - PySpark jobs for data cleaning/enrichment
- `transform/` - dbt project (5-layer modeling)
  - `models/staging/` - Raw data validation + light transforms
  - `models/intermediate/` - Feature engineering
  - `models/marts/` - Business-facing tables (daily summaries, anomaly flags)
- `orchestration/` - Airflow DAGs and plugins
  - `dags/` - hourly_ingestion_dag.py, daily_transform_dag.py
  - `plugins/sensors/` - Custom blob_sensor.py (monitors Azure Blob Storage)
- `infra/` - Infrastructure as Code
  - `azure/` - Terraform for cloud resources (storage, networks)
  - `services/` - Terraform for local services (Kafka, PostgreSQL, etc.)
  - `postgres/` - Database initialization scripts
- `viz/` - Superset configuration and dashboard exports
- `scripts/` - Setup and utility scripts (generate_secrets.sh, tunnel_db.sh)

## Local Development

### Prerequisites

- Docker and Docker Compose
- Python 3.8+ (for running producers/consumers locally)
- Terraform (optional, for cloud provisioning)

### Starting the Stack

```bash
make up                # Start all services (Airflow, Kafka, Postgres, Superset)
make ps                # View running containers
make logs              # Tail logs from all services
make down              # Stop all services
```

**Service URLs after `make up`:**
- Airflow UI: `http://localhost:8080` (admin/admin)
- Kafka UI: `http://localhost:8090`
- Superset: `http://localhost:8088` (admin/admin)
- PostgreSQL: `localhost:5432` (airflow/airflow)

### Configuration

Copy `.env.example` to `.env` and populate required variables:

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:29092

# Airflow
AIRFLOW_FERNET_KEY=<generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
AIRFLOW_SECRET_KEY=<any random string>

# Azure (for cloud features)
AZURE_STORAGE_ACCOUNT_NAME=<account>
AZURE_STORAGE_ACCOUNT_KEY=<key>

# ENTSOE-E API (energy data)
ENTSOE_API_KEY=<your_api_key>

# Superset
SUPERSET_SECRET_KEY=<random_string>
```

### Data Flow & Pipeline Stages

1. **Ingestion (Kafka → Azure ADLS Bronze)**
   - **Hourly Ingestion DAG** (`orchestration/dags/hourly_ingestion_dag.py`)
   - Triggers `weather_producer` → publishes to `raw.weather.hourly` Kafka topic
   - Triggers `energy_producer` → publishes to `raw.energy.demand` Kafka topic
   - **Kafka Consumers** (run continuously)
     - Read from Kafka topics
     - Batch messages (100 records per batch for efficiency)
     - Write NDJSON blobs to Azure ADLS Bronze layer, date-partitioned
     - Commit Kafka offsets only after successful blob write (at-least-once semantics)

2. **Processing (Bronze → Silver, optional)**
   - **Processing jobs** (PySpark, scheduled via Airflow)
   - Read raw NDJSON from Bronze layer
   - Clean, validate, deduplicate, enrich
   - Write to Silver layer (parquet format recommended)
   - Currently placeholder; implement based on quality requirements

3. **Transform (Silver/Bronze → PostgreSQL)**
   - **Daily Transform DAG** (`orchestration/dags/daily_transform_dag.py`)
   - Read from Bronze/Silver layers in Azure
   - Execute `dbt run` to build models:
     - Staging → light transforms, validation
     - Intermediate → joins, aggregations
     - Marts → business-ready tables (daily summaries, anomaly flags)
   - Load into PostgreSQL analytics schema

## Development Workflows

### Running Tests

```bash
make test              # Run pytest on ingestion/ and processing/
make lint              # Run ruff linter on Python code
```

### Working with dbt

```bash
make dbt-run           # Execute all dbt models in dependency order
make dbt-test          # Run dbt tests (schema + data quality tests)
make dbt-docs          # Generate + serve dbt documentation (http://localhost:8000)
cd transform && dbt run --select tag:staging  # Run only staging models
cd transform && dbt run --models +stg_weather # Run model + downstream
```

**dbt Structure:**
- **Staging** (`models/staging/stg_*.sql`) - Validate + light transforms on raw data
- **Intermediate** (`models/intermediate/int_*.sql`) - Join/aggregate for features
- **Marts** (`models/marts/mart_*.sql`) - Final business tables

### Running Producers & Consumers

**Producers** publish raw data to Kafka topics. Consumers listen continuously and write to Azure ADLS Bronze layer.

```bash
# Start consumers first (they listen for Kafka messages and write to Azure)
python -m ingestion.consumers.weather_consumer    # Reads raw.weather.hourly → bronze/weather/...
python -m ingestion.consumers.energy_consumer      # Reads raw.energy.demand → bronze/energy/...

# In another shell, run producers to publish data to Kafka
# Weather data backfill (pulls 2025-01-01 to today-5 days)
python ingestion/producers/weather_backfill.py

# Incremental weather (latest finalized day, normally run daily in Airflow)
python ingestion/producers/weather_producer.py

# Energy data (from ENTSOE-E API)
python ingestion/producers/energy_producer.py
```

**Note**: Consumers batch messages (default 100 records) before writing to Azure. If you don't see files in Azure ADLS immediately, wait for the batch to fill or for a timeout (5 seconds of no new messages triggers a flush).

### Cloud Deployment

```bash
make tf-init           # Initialize Terraform for dev + prod
make tf-plan-dev       # Preview dev environment changes
make tf-apply-dev      # Deploy dev resources to Azure
make tf-plan-prod      # Preview prod environment changes
make tf-apply-prod     # Deploy prod resources to Azure
make tf-output-dev     # Get Terraform outputs as .env values
make tunnel-db         # SSH tunnel to prod PostgreSQL (localhost:5433)
```

## Kafka Topics & Message Format

### Topics Created on Startup

- `raw.weather.hourly` (3 partitions, 7-day retention)
  ```json
  {
    "city_name": "Berlin",
    "city": "DE",
    "latitude": 52.52,
    "longitude": 13.40,
    "time": "2025-01-15T14:00Z",
    "temperature": 5.2,
    "humidity": 78,
    "wind_speed": 12.5,
    "precipitation": 0.0,
    "weather_code": 2,
    "weather": "Partly cloudy"
  }
  ```

- `raw.energy.demand` (3 partitions, 7-day retention)
  ```json
  {
    "country": "DE",
    "timestamp": "2025-01-15T14:00Z",
    "demand_mw": 45230,
    "forecast_mw": 45100
  }
  ```

- `dead.letter.queue` (1 partition, 30-day retention) - Failed messages

### Debugging Kafka

```bash
make ps                # Check kafka, zookeeper, kafka-ui containers
# View in UI: http://localhost:8090
```

## Code Patterns & Conventions

### Python Style

- Use `ruff` for linting: `make lint`
- Format: Black-compatible style (enforced by ruff)
- Logging: Use `logging` module, not print()
- Kafka producers/consumers: Use `confluent_kafka` library

### dbt Conventions

- Naming: `stg_` (staging), `int_` (intermediate), `mart_` (marts)
- Materialization: `table` for marts, `view` for staging/intermediate
- Tests: Define in `schema.yml` files alongside models
- Docs: Use `description` and `meta` fields for lineage

### Terraform

- Separate state for `infra/azure` (cloud) and `infra/services` (local)
- Use `secrets.tfvars` for sensitive data (Git-ignored, never commit)
- Environment-specific vars: `envs/dev.tfvars`, `envs/prod.tfvars`

## Gotchas & Important Notes

### Weather Data

- Open-Meteo API provides **historical data only** (5+ days old)
- Incremental mode always pulls the most recent finalized day (today-5)
- Run `weather_backfill.py` once on first setup to load historical data
- Timestamps are ISO 8601 UTC format

### Azure ADLS & Kafka Consumers

- Consumers batch messages (100 records default) before writing to Azure
- If files don't appear immediately in ADLS, wait for batch to fill or the 5-second timeout (no messages)
- NDJSON format: each line is a valid JSON object (newline-delimited)
- Date partitioning (`year=*/month=*/day=*`) allows efficient date-range queries in dbt/Spark
- Consumers commit Kafka offsets **only after successful blob write** (at-least-once semantics)
  - If a consumer crashes mid-batch, it will re-process those messages on restart
  - The `overwrite=True` flag in blob upload prevents duplicates if blobs are identical

### Medallion Architecture

- **Bronze**: Raw, as-ingested data — minimal transformation, full lineage preserved
- **Silver**: Cleaned and validated — implement quality checks in `processing/` PySpark jobs
- **Gold**: Business-ready, aggregated — dbt models and analytics queries
- Always read from Silver/Gold for analytics; Bronze is for debugging and lineage

### Airflow Local Executor

- Only one task runs at a time (no parallelism)
- Good for dev; production should use Celery or Kubernetes
- DAGs auto-pause on creation; manually enable in Airflow UI

### Data Layers

### Azure ADLS Containers

- **Bronze** - Raw, ingested data (NDJSON format, date-partitioned)
  - `weather/year=2025/month=01/day=15/DE_*.json`
  - `energy/year=2025/month=01/day=15/DE_*.json`
  - Written by Kafka consumers, read by processing/dbt jobs

- **Silver** (optional, currently placeholder)
  - Cleaned, validated, deduplicated data
  - Implement PySpark jobs in `processing/` to read Bronze → write Silver
  - Use Parquet format for better query performance

- **Gold** (final layer before analytics)
  - Pre-aggregated, business-ready tables
  - Can be dbt models reading from Silver, or exported to PostgreSQL

### PostgreSQL Schema

- Airflow creates `public` schema for its own metadata tables
- dbt writes analytics models to `public` schema (or configure `target_schema` in `dbt_project.yml`)
- Data flows: Bronze/Silver in Azure ADLS → dbt transforms → PostgreSQL analytics tables → Superset dashboards

### Missing Secrets

- `.env` file is not in Git (use `.env.example`)
- Generate `AIRFLOW_FERNET_KEY`: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
- Azure credentials required:
  - `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY` needed for consumers to write to ADLS
  - Without these, Kafka consumers will crash when trying to flush batches
- Without proper secrets, Airflow/Superset containers and data ingestion will fail

## Useful Commands Reference

| Task | Command |
|------|---------|
| View all services | `make ps` |
| Tail logs | `make logs` |
| Run Python tests | `make test` |
| Lint Python code | `make lint` |
| Run all dbt models | `make dbt-run` |
| Test dbt models | `make dbt-test` |
| View dbt docs | `make dbt-docs` |
| Init Terraform | `make tf-init` |
| Plan dev infra | `make tf-plan-dev` |
| Deploy dev infra | `make tf-apply-dev` |
| Tunnel to prod DB | `make tunnel-db` |

## Testing & Validation

- **Unit tests**: `ingestion/tests/`, `processing/tests/` (run with `make test`)
- **dbt tests**: Define in `models/*/schema.yml`; run with `make dbt-test`
- **Manual validation**: Use Kafka UI (http://localhost:8090) to inspect messages
- **Data quality**: Check `mart_anomaly_flags` table for outlier detection

## Next Steps / TODOs

- **Complete Airflow DAGs**: `hourly_ingestion_dag.py` and `daily_transform_dag.py` are currently minimal; implement full task dependencies
- **Implement Silver Layer**: Create PySpark jobs in `processing/` to:
  - Read Bronze NDJSON from Azure ADLS
  - Apply data quality checks, deduplication, schema validation
  - Write Parquet files to Silver layer
- **dbt Data Quality**: Add data quality tests to `models/*/schema.yml` (uniqueness, not_null, relationships, custom SQL tests)
- **Superset Dashboards**: Configure dashboards to visualize anomaly flags and daily demand summaries
- **Alerting**: Set up email/Slack notifications for:
  - Pipeline failures (Airflow email alerts)
  - Data quality issues (dbt test failures)
  - Anomalies detected in energy demand
