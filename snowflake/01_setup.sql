-- Part 4: make the Iceberg tables in S3 readable from Snowflake.
-- Run once in a Snowflake worksheet as ACCOUNTADMIN (trial account).
-- Replace roopa-nyc-taxi-448716, 448716790732 and ROOPAPUTTA.
--
-- Approach: external volume on s3://roopa-nyc-taxi-448716/warehouse/ + OBJECT_STORE catalog
-- integration. Snowflake reads the Iceberg metadata.json files directly from S3,
-- so it does not need network access to the local Polaris server.

USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS taxi_role;
CREATE WAREHOUSE IF NOT EXISTS taxi_wh
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
CREATE DATABASE IF NOT EXISTS taxi_db;
CREATE SCHEMA IF NOT EXISTS taxi_db.taxi;

-- 1. External volume: Snowflake assumes the read-only IAM role created by infra/setup_aws.sh
CREATE EXTERNAL VOLUME IF NOT EXISTS taxi_iceberg_vol
  STORAGE_LOCATIONS = ((
    NAME = 'taxi-s3-warehouse'
    STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = 's3://roopa-nyc-taxi-448716/warehouse/'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::448716790732:role/taxi-snowflake-read'
  ))
  ALLOW_WRITES = FALSE;

-- 2. Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from the output into
--    infra/iam/snowflake-trust-policy.json, then (as your AWS admin):
--      aws iam update-assume-role-policy --role-name taxi-snowflake-read \
--        --policy-document file://infra/iam/snowflake-trust-policy.json
DESC EXTERNAL VOLUME taxi_iceberg_vol;

-- 3. Check that Snowflake can reach the bucket (should return "success": true)
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('taxi_iceberg_vol');

-- 4. Catalog integration for Iceberg tables whose metadata lives in object storage
CREATE CATALOG INTEGRATION IF NOT EXISTS taxi_objstore_cat
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT = ICEBERG
  ENABLED = TRUE;

-- 5. Permissions for the working role
GRANT USAGE ON WAREHOUSE taxi_wh TO ROLE taxi_role;
GRANT USAGE ON DATABASE taxi_db TO ROLE taxi_role;
GRANT ALL ON SCHEMA taxi_db.taxi TO ROLE taxi_role;
GRANT USAGE ON EXTERNAL VOLUME taxi_iceberg_vol TO ROLE taxi_role;
GRANT USAGE ON INTEGRATION taxi_objstore_cat TO ROLE taxi_role;
GRANT ROLE taxi_role TO USER ROOPAPUTTA;

-- 6. Tables: run `python -m jobs.snowflake_metadata_paths` and execute the generated
--    snowflake/02_tables_generated.sql. It looks like:
--
--    CREATE ICEBERG TABLE IF NOT EXISTS trips_clean
--      EXTERNAL_VOLUME = 'taxi_iceberg_vol'
--      CATALOG = 'taxi_objstore_cat'
--      METADATA_FILE_PATH = 'taxi/trips_clean/metadata/00004-....metadata.json';
