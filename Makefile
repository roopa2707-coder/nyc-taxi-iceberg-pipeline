# Every part of the exercise is one command. Run from the repo root with .env filled in.
PY ?= python
MAX_DAY ?=

.PHONY: install test ingest polaris-up polaris-setup polaris-down load features metrics dq \
        snowflake-sql export app all demo-incremental

install:          ## Python dependencies (Java 17 must be installed for Spark)
	$(PY) -m pip install -r requirements.txt

test:             ## Part 5: pytest unit tests
	$(PY) -m pytest -v

ingest:           ## Part 1: download TLC files and land them in s3://$$S3_BUCKET/raw/
	$(PY) scripts/ingest_to_s3.py

polaris-up:       ## start local Apache Polaris
	docker compose --env-file .env -f docker/docker-compose.yml up -d --wait

polaris-setup:    ## create the Iceberg catalog in Polaris
	$(PY) scripts/setup_polaris.py

polaris-down:
	docker compose --env-file .env -f docker/docker-compose.yml down

load:             ## Part 2 + 3.1: raw -> trips_clean / trips_rejected with MERGE INTO (MAX_DAY=15 for a partial load)
	$(PY) -m jobs.build_trips_clean $(if $(MAX_DAY),--max-day $(MAX_DAY),)

features:         ## Part 3.2 + 3.3: schema evolution + time travel
	$(PY) -m jobs.iceberg_features

metrics:          ## Part 3.4: daily_zone_metrics
	$(PY) -m jobs.build_daily_zone_metrics

dq:               ## Part 5: data quality checks (exit 1 on failure)
	$(PY) -m jobs.run_quality_checks

snowflake-sql:    ## Part 4: generate CREATE/REFRESH ICEBERG TABLE statements for Snowflake
	$(PY) -m jobs.snowflake_metadata_paths

export:           ## Part 4: run snowflake/queries.sql and save results/*.csv
	$(PY) scripts/export_snowflake_results.py

app:              ## results viewer
	streamlit run app/streamlit_app.py

demo-incremental: ## the exact load sequence used for the Part 3 evidence
	$(MAKE) load MAX_DAY=15   # snapshot 1: first half of the month
	$(MAKE) load              # snapshot 2: MERGE inserts only the second half
	$(MAKE) load              # re-run: 0 rows inserted (idempotent)

all: ingest demo-incremental features metrics dq snowflake-sql
