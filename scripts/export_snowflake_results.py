"""Run snowflake/queries.sql against Snowflake and save each result to results/<name>.csv.

    python scripts/export_snowflake_results.py

Connection settings come from env vars (.env): SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER,
SNOWFLAKE_PASSWORD. The CSVs are committed so the Streamlit app never needs credentials.
"""
from __future__ import annotations

import csv
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import pipeline.config  # noqa: E402,F401  (loads .env)


def parse_named_queries(sql_text: str) -> tuple[list[str], dict[str, str]]:
    """Split the file into setup statements (before the first '-- name:') and named queries."""
    parts = re.split(r"^--\s*name:\s*(\w+)\s*$", sql_text, flags=re.MULTILINE)
    setup = [s.strip() for s in parts[0].split(";") if _strip_comments(s)]
    queries = {}
    for name, body in zip(parts[1::2], parts[2::2]):
        queries[name] = body.strip().rstrip(";")
    return setup, queries


def _strip_comments(s: str) -> str:
    return "\n".join(line for line in s.splitlines() if not line.strip().startswith("--")).strip()


def main() -> None:
    import snowflake.connector

    setup, queries = parse_named_queries((ROOT / "snowflake" / "queries.sql").read_text())
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
    )
    out_dir = ROOT / "results"
    out_dir.mkdir(exist_ok=True)
    try:
        cur = conn.cursor()
        for stmt in setup:
            cur.execute(_strip_comments(stmt))
        for name, sql in queries.items():
            cur.execute(sql)
            cols = [d[0].lower() for d in cur.description]
            rows = cur.fetchall()
            path = out_dir / f"{name}.csv"
            with path.open("w", newline="") as f:
                w = csv.writer(f)
                w.writerow(cols)
                w.writerows(rows)
            print(f"{name}: {len(rows)} rows -> {path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
