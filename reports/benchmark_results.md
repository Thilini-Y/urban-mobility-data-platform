# Task 6 — Storage Strategy Benchmark

Two partitioning strategies for the `tripdata` silver table, benchmarked on ingestion (write) time, storage size, file count, and query latency for the three required queries.

| Metric | A: partitioned by (pickup_year, pickup_month) | B: partitioned by (event_date) |
|---|---|---|
| Write time (s) | 7.76 | 5.39 |
| Storage size (MB) | 891.91 | 844.45 |
| Number of files | 13 | 95 |
| Q1: number of taxi trips per borough (s) | 0.738 | 0.334 |
| Q2: average trip duration per day (s) | 0.315 | 0.423 |
| Q3: average fare per borough (s) | 0.309 | 0.315 |
