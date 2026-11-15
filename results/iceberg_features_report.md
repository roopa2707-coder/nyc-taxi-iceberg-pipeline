# Iceberg features evidence

## Snapshot history

|         snapshot_id | committed_at               | operation   |   added_records |   total_records |
|--------------------:|:---------------------------|:------------|----------------:|----------------:|
| 8057421087991349286 | 2026-11-15 15:54:40.398000 | append      |   nan           |               0 |
| 1781206122456224604 | 2026-11-15 15:55:10.020000 | append      |     1.48262e+06 |         1482619 |
|   12587078615615486 | 2026-11-15 15:56:10.557000 | append      |     1.84695e+06 |         3329565 |
| 3618773894970406107 | 2026-11-15 15:57:00.989000 | append      |   nan           |         3329565 |

## Schema evolution

Column `is_airport_trip` already existed (job was re-run).

- data files before ALTER: 31, after ALTER: 31
- current snapshot before ALTER: 3618773894970406107, after ALTER: 3618773894970406107

Same files and same snapshot: the change is metadata-only, no data was rewritten. Existing rows read the new column as NULL; later loads populate it.

## is_airport_trip values in the current table

| is_airport_trip   |   trips |
|:------------------|--------:|
|                   | 3329565 |

## Time travel: row counts before and after the change

| version             |         snapshot_id |   row_count | min_pickup_date   | max_pickup_date   |
|:--------------------|--------------------:|------------:|:------------------|:------------------|
| first data snapshot | 1781206122456224604 |     1482619 | 2025-01-01        | 2025-01-15        |
| current             | 3618773894970406107 |     3329565 | 2025-01-01        | 2025-01-31        |

## Columns visible at the first snapshot (time travel uses that snapshot's schema)

trip_id, pickup_date, pickup_hour, vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, ratecode_id, store_and_fwd_flag, pu_location_id, do_location_id, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee, cbd_congestion_fee, payment_type_desc, pickup_borough, pickup_zone, dropoff_borough, dropoff_zone, trip_minutes, avg_mph, tip_pct, source_file
