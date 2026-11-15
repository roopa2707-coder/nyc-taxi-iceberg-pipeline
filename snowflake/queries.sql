-- Part 4 analysis queries (Snowflake). Each block starts with "-- name:" so
-- scripts/export_snowflake_results.py can run them and save results/<name>.csv.
USE ROLE taxi_role;
USE WAREHOUSE taxi_wh;
USE SCHEMA taxi_db.taxi;

-- name: q1_top10_pickup_zones_by_revenue
-- Which 10 pickup zones earn the most revenue? (revenue = total_amount, all fees included)
SELECT
    pickup_borough,
    pickup_zone,
    SUM(trips)                    AS trips,
    ROUND(SUM(total_revenue), 2)  AS total_revenue,
    ROUND(SUM(total_revenue) / SUM(trips), 2) AS revenue_per_trip
FROM daily_zone_metrics
GROUP BY pickup_borough, pickup_zone
ORDER BY total_revenue DESC
LIMIT 10;

-- name: q2_trip_demand_by_hour
-- How does trip demand vary by hour of day? (pickup hour, NYC local time as recorded by TLC)
SELECT
    pickup_hour,
    COUNT(*)                                                AS trips,
    ROUND(COUNT(*) / COUNT(DISTINCT pickup_date), 0)        AS avg_trips_per_day,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)        AS pct_of_all_trips
FROM trips_clean
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- name: q3_avg_tip_pct_by_payment_type
-- What is the average tip percentage by payment type? (tip_pct = tip_amount / fare_amount * 100)
-- Note: TLC only records card tips, so cash trips show ~0% by construction.
SELECT
    payment_type,
    payment_type_desc,
    COUNT(*)                   AS trips,
    ROUND(AVG(tip_pct), 2)     AS avg_tip_pct,
    ROUND(MEDIAN(tip_pct), 2)  AS median_tip_pct
FROM trips_clean
GROUP BY payment_type, payment_type_desc
ORDER BY trips DESC;
