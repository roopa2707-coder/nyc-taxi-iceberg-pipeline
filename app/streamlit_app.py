"""Results viewer for the NYC taxi pipeline.

Shows the pre-exported Snowflake query results committed in results/ - the app
never connects to Snowflake or AWS, so no credentials are needed or exposed.

    streamlit run app/streamlit_app.py
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

RESULTS = Path(__file__).resolve().parent.parent / "results"

st.set_page_config(page_title="NYC Taxi Demand & Revenue", layout="wide")
st.title("NYC Yellow Taxi: demand and revenue by zone")
st.caption(
    "Pipeline: TLC Parquet → S3 (raw) → PySpark → Apache Iceberg (Polaris catalog) → Snowflake. "
    "Figures below are exported from the Snowflake queries in snowflake/queries.sql."
)


def load(name: str) -> pd.DataFrame | None:
    path = RESULTS / f"{name}.csv"
    if not path.exists():
        st.warning(f"{path.name} not found - run scripts/export_snowflake_results.py and commit results/.")
        return None
    return pd.read_csv(path)


st.header("1. Top 10 pickup zones by revenue")
top = load("q1_top10_pickup_zones_by_revenue")
if top is not None:
    top["zone"] = top["pickup_zone"] + " (" + top["pickup_borough"] + ")"
    import altair as alt

    chart = (
        alt.Chart(top)
        .mark_bar()
        .encode(
            x=alt.X("total_revenue:Q", title="Total revenue (USD)"),
            y=alt.Y("zone:N", sort="-x", title=None),  # highest revenue at the top
            tooltip=["zone", "trips", "total_revenue", "revenue_per_trip"],
        )
    )
    st.altair_chart(chart, use_container_width=True)
    st.dataframe(top.drop(columns="zone"), hide_index=True, use_container_width=True)

st.header("2. Trip demand by hour of day")
hours = load("q2_trip_demand_by_hour")
if hours is not None:
    st.bar_chart(hours.set_index("pickup_hour")["avg_trips_per_day"])
    st.dataframe(hours, hide_index=True, use_container_width=True)

st.header("3. Average tip % by payment type")
tips = load("q3_avg_tip_pct_by_payment_type")
if tips is not None:
    st.dataframe(tips, hide_index=True, use_container_width=True)
    st.caption("TLC records only card tips, so cash trips show ~0% tip by construction.")

with st.expander("Data quality report"):
    dq = RESULTS / "dq_report.json"
    if dq.exists():
        st.dataframe(pd.DataFrame(json.loads(dq.read_text())), hide_index=True, use_container_width=True)
    else:
        st.info("results/dq_report.json not found.")

with st.expander("Iceberg features evidence (MERGE, schema evolution, time travel)"):
    rep = RESULTS / "iceberg_features_report.md"
    st.markdown(rep.read_text() if rep.exists() else "results/iceberg_features_report.md not found.")
