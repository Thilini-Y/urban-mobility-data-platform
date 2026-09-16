# Week 2 — Analytical Query Results

## Q1_monthly_demand_per_zone

| pickup_zone                  |   pickup_year |   pickup_month |   trip_count |
|:-----------------------------|--------------:|---------------:|-------------:|
| JFK Airport                  |          2024 |              1 |       144077 |
| Upper East Side South        |          2024 |              1 |       141577 |
| Midtown Center               |          2024 |              1 |       141355 |
| Upper East Side North        |          2024 |              1 |       135679 |
| Midtown East                 |          2024 |              1 |       105492 |
| Times Sq/Theatre District    |          2024 |              1 |       104750 |
| Penn Station/Madison Sq West |          2024 |              1 |       103833 |
| Lincoln Square East          |          2024 |              1 |       102853 |
| LaGuardia Airport            |          2024 |              1 |        88623 |
| Upper West Side South        |          2024 |              1 |        87902 |
| Midtown North                |          2024 |              1 |        84555 |
| Murray Hill                  |          2024 |              1 |        82243 |
| East Chelsea                 |          2024 |              1 |        76866 |
| Clinton East                 |          2024 |              1 |        75631 |
| Union Sq                     |          2024 |              1 |        75061 |
| Lenox Hill West              |          2024 |              1 |        73410 |
| Lenox Hill East              |          2024 |              1 |        65636 |
| West Village                 |          2024 |              1 |        65424 |
| East Village                 |          2024 |              1 |        65218 |
| Midtown South                |          2024 |              1 |        64105 |

## Q2_avg_distance_by_weather

| weather_condition   |   weather_condition_code |   trip_count |   avg_trip_distance_miles |
|:--------------------|-------------------------:|-------------:|--------------------------:|
| Fair                |                        2 |      3027082 |                     4.281 |
| Cloudy              |                        3 |      2355419 |                     3.948 |
| Overcast            |                        4 |      2066447 |                     4.04  |
| Light Rain          |                        7 |       743432 |                     3.811 |
| Heavy Rain          |                        9 |       402901 |                     4.243 |
| Rain                |                        8 |       253824 |                     3.719 |
| Fog                 |                        5 |       235549 |                     3.53  |
| Clear               |                        1 |       227418 |                     3.8   |
| Light Snowfall      |                       14 |       143948 |                     3.311 |
| Snowfall            |                       15 |        22219 |                     3.038 |
| Sleet               |                       12 |        17209 |                     3.248 |
| Heavy Sleet         |                       13 |        16724 |                     3.055 |
| Heavy Snowfall      |                       16 |        15848 |                     2.371 |
| Freezing Rain       |                       10 |        10026 |                     6.734 |

## Q3_air_quality_vs_demand

| pm25_level                                    |   trip_count |   avg_pm25 |
|:----------------------------------------------|-------------:|-----------:|
| 1: Good (0.0-9.0)                             |       720334 |       4.81 |
| 2: Moderate (9.1-35.4)                        |       233234 |      15.27 |
| 3: Unhealthy for Sensitive Groups (35.5-55.4) |         1828 |      37.55 |

## Q4_zones_most_weather_sensitive

| pickup_zone                  |   conditions_observed |   demand_stddev |   min_demand |   max_demand |
|:-----------------------------|----------------------:|----------------:|-------------:|-------------:|
| Midtown Center               |                    14 |         49159.7 |          412 |       140906 |
| Upper East Side South        |                    14 |         47734.1 |          676 |       138877 |
| JFK Airport                  |                    14 |         46056.8 |          443 |       140254 |
| Upper East Side North        |                    14 |         45518.8 |          650 |       135354 |
| Midtown East                 |                    14 |         36024   |          376 |       105039 |
| Times Sq/Theatre District    |                    14 |         34741   |          257 |       104078 |
| Penn Station/Madison Sq West |                    14 |         34147.1 |          369 |       103248 |
| Lincoln Square East          |                    14 |         33307.3 |          309 |       100595 |
| LaGuardia Airport            |                    14 |         31842   |          106 |        96203 |
| Upper West Side South        |                    14 |         30662.3 |          310 |        94012 |

## Q5_peak_hour_per_day

| day_of_week   |   peak_hour |   trip_count |
|:--------------|------------:|-------------:|
| Sunday        |           0 |        97535 |
| Monday        |          22 |        79266 |
| Tuesday       |          23 |        97242 |
| Wednesday     |          23 |       108917 |
| Thursday      |          23 |       119133 |
| Friday        |           0 |       105205 |
| Saturday      |          23 |        98696 |

## Q6_monthly_trends

|   pickup_year |   pickup_month |   trip_count |   avg_fare |   avg_distance |
|--------------:|---------------:|-------------:|-----------:|---------------:|
|          2024 |              1 |  2.93654e+06 |      18.19 |           3.66 |
|          2024 |              2 |  2.99537e+06 |      18.04 |           3.86 |
|          2024 |              3 |  3.60613e+06 |      18.66 |           4.5  |
