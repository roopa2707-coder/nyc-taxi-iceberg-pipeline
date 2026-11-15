import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("export", ROOT / "scripts" / "export_snowflake_results.py")
export = importlib.util.module_from_spec(spec)
spec.loader.exec_module(export)


def test_queries_sql_has_the_three_required_queries():
    setup, queries = export.parse_named_queries((ROOT / "snowflake" / "queries.sql").read_text())
    assert list(queries) == [
        "q1_top10_pickup_zones_by_revenue",
        "q2_trip_demand_by_hour",
        "q3_avg_tip_pct_by_payment_type",
    ]
    assert any("USE WAREHOUSE" in s for s in setup)
    assert all(q.upper().lstrip().startswith(("SELECT", "--")) for q in queries.values())
