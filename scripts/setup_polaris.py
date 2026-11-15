"""Create the Iceberg catalog in the local Apache Polaris server (Docker).

    python scripts/setup_polaris.py

Steps (all idempotent - re-running is fine):
  1. get an OAuth token for the root principal
  2. create catalog POLARIS_CATALOG with storage s3://<bucket>/warehouse/
  3. grant CATALOG_MANAGE_CONTENT to the catalog_admin role so the root
     principal (through service_admin) can create and write tables
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.config import load_config  # noqa: E402


def main() -> None:
    cfg = load_config()
    if not cfg.polaris_client_secret:
        sys.exit("POLARIS_CLIENT_SECRET is not set")
    base = cfg.polaris_uri.rsplit("/api/catalog", 1)[0]
    role_arn = os.getenv("POLARIS_S3_ROLE_ARN")
    if not role_arn:
        sys.exit("POLARIS_S3_ROLE_ARN is not set (see README, IAM section)")

    tok = requests.post(
        f"{base}/api/catalog/v1/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": cfg.polaris_client_id,
            "client_secret": cfg.polaris_client_secret,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        timeout=30,
    )
    tok.raise_for_status()
    h = {"Authorization": f"Bearer {tok.json()['access_token']}", "Content-Type": "application/json"}

    location = f"s3://{cfg.s3_bucket}/warehouse/"
    body = {
        "catalog": {
            "name": cfg.polaris_catalog,
            "type": "INTERNAL",
            "properties": {"default-base-location": location},
            "storageConfigInfo": {
                "storageType": "S3",
                "roleArn": role_arn,
                "allowedLocations": [location],
                "region": cfg.aws_region,
            },
        }
    }
    r = requests.post(f"{base}/api/management/v1/catalogs", json=body, headers=h, timeout=30)
    if r.status_code == 409:
        print(f"catalog {cfg.polaris_catalog} already exists")
    else:
        r.raise_for_status()
        print(f"created catalog {cfg.polaris_catalog} at {location}")

    grant = {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}
    r = requests.put(
        f"{base}/api/management/v1/catalogs/{cfg.polaris_catalog}/catalog-roles/catalog_admin/grants",
        json=grant,
        headers=h,
        timeout=30,
    )
    r.raise_for_status()
    print("granted CATALOG_MANAGE_CONTENT to catalog_admin")


if __name__ == "__main__":
    main()
