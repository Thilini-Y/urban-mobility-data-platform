# Task 6 — Storage Strategy Benchmark

Two partitioning strategies for the `tripdata` silver table, benchmarked on ingestion (write) time, storage size, file count, and query latency for the three required queries.

| Metric | A: partitioned by (pickup_year, pickup_month) | B: partitioned by (event_date) |
|---|---|---|
| Ingestion time (s) | 6.65 | 5.08 |
| Storage size (MB) | 890.06 | 845.65 |
| Number of files | 10 | 91 |
| Q1: number of taxi trips per borough (s) | 0.188 | 0.145 |
| Q2: average trip duration per day (s) | 0.065 | 0.070 |
| Q3: average fare per borough (s) | 0.183 | 0.133 |
