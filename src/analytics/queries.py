# Based on https://dev.meteostat.net/formats weather condition codes
WEATHER_CONDITION_CASE = """
    CASE weather_condition_code
        WHEN 1 THEN 'Clear'
        WHEN 2 THEN 'Fair'
        WHEN 3 THEN 'Cloudy'
        WHEN 4 THEN 'Overcast'
        WHEN 5 THEN 'Fog'
        WHEN 6 THEN 'Freezing Fog'
        WHEN 7 THEN 'Light Rain'
        WHEN 8 THEN 'Rain'
        WHEN 9 THEN 'Heavy Rain'
        WHEN 10 THEN 'Freezing Rain'
        WHEN 11 THEN 'Heavy Freezing Rain'
        WHEN 12 THEN 'Sleet'
        WHEN 13 THEN 'Heavy Sleet'
        WHEN 14 THEN 'Light Snowfall'
        WHEN 15 THEN 'Snowfall'
        WHEN 16 THEN 'Heavy Snowfall'
        WHEN 17 THEN 'Rain Shower'
        WHEN 18 THEN 'Heavy Rain Shower'
        WHEN 19 THEN 'Sleet Shower'
        WHEN 20 THEN 'Heavy Sleet Shower'
        WHEN 21 THEN 'Snow Shower'
        WHEN 22 THEN 'Heavy Snow Shower'
        WHEN 23 THEN 'Lightning'
        WHEN 24 THEN 'Hail'
        WHEN 25 THEN 'Thunderstorm'
        WHEN 26 THEN 'Heavy Thunderstorm'
        WHEN 27 THEN 'Storm'
        ELSE 'Unknown'
    END
"""

# Based on https://www.epa.gov/system/files/documents/2024-02/pm-naaqs-air-quality-index-fact-sheet.pdf
PM25_LEVEL_CASE = """
    CASE
        WHEN GREATEST(air_quality_pm25, 0) <= 9.0 THEN '1: Good (0.0-9.0)'
        WHEN air_quality_pm25 <= 35.4 THEN '2: Moderate (9.1-35.4)'
        WHEN air_quality_pm25 <= 55.4 THEN '3: Unhealthy for Sensitive Groups (35.5-55.4)'
        WHEN air_quality_pm25 <= 125.4 THEN '4: Unhealthy (55.5-125.4)'
        WHEN air_quality_pm25 <= 225.4 THEN '4: Very Unhealthy (125.4-255.4)'
        ELSE '5: Hazardous (255.4+)'
    END
"""


# Q1: Monthly taxi demand for each taxi zone
QUERY_MONTHLY_DEMAND_PER_ZONE = f"""
SELECT pickup_zone, pickup_year, pickup_month, COUNT(*) AS trip_count
FROM integrated_taxi_trips
GROUP BY pickup_zone, pickup_year, pickup_month
ORDER BY pickup_year, pickup_month, trip_count DESC
"""

# Q2: Average trip distance under different weather conditions
QUERY_AVG_DISTANCE_BY_WEATHER = f"""
SELECT
    {WEATHER_CONDITION_CASE} AS weather_condition,
    weather_condition_code,
    COUNT(*) AS trip_count,
    ROUND(AVG(trip_distance), 3) AS avg_trip_distance_miles
FROM integrated_taxi_trips
WHERE weather_condition_code IS NOT NULL
GROUP BY 1, 2
ORDER BY trip_count DESC
"""

# Q3: Relationship between air quality and taxi demand
QUERY_AIR_QUALITY_VS_DEMAND = f"""
SELECT
    {PM25_LEVEL_CASE} AS pm25_level,
    COUNT(*) AS trip_count,
    ROUND(AVG(air_quality_pm25), 2) AS avg_pm25
FROM integrated_taxi_trips
WHERE air_quality_pm25 IS NOT NULL
GROUP BY 1
ORDER BY 1
"""

# Q4: Taxi zones with the largest demand variation across weather
QUERY_ZONES_MOST_WEATHER_SENSITIVE = f"""
WITH zone_weather_demand AS (
    SELECT
        pickup_zone,
        {WEATHER_CONDITION_CASE} AS weather_condition,
        COUNT(*) AS trip_count
    FROM integrated_taxi_trips
    WHERE pickup_zone IS NOT NULL AND weather_condition_code IS NOT NULL
    GROUP BY 1, 2
)
SELECT
    pickup_zone,
    COUNT(DISTINCT weather_condition) AS conditions_observed,
    ROUND(STDDEV(trip_count), 2) AS demand_stddev,
    MIN(trip_count) AS min_demand,
    MAX(trip_count) AS max_demand
FROM zone_weather_demand
GROUP BY pickup_zone
HAVING COUNT(DISTINCT weather_condition) >= 2
ORDER BY demand_stddev DESC
LIMIT 10
"""

# Q5: Peak travel hour for each day of the week
QUERY_PEAK_HOUR_PER_DAY = """
WITH hourly_counts AS (
    SELECT
        date_format(event_timestamp, 'EEEE') AS day_of_week,
        dayofweek(event_timestamp) AS day_num,
        event_hour,
        COUNT(*) AS trip_count
    FROM integrated_taxi_trips
    GROUP BY 1, 2, 3
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY day_of_week ORDER BY trip_count DESC) AS hour_rank
    FROM hourly_counts
)
SELECT day_of_week, event_hour AS peak_hour, trip_count
FROM ranked
WHERE hour_rank = 1
ORDER BY day_num
"""

# Q6: Monthly trends in taxi demand
QUERY_MONTHLY_TRENDS = """
SELECT
    pickup_year, pickup_month,
    COUNT(*) AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance
WHERE pickup_year = 2024 AND pickup_month = 4
FROM integrated_taxi_trips
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month
"""


ALL_QUERIES = {
    "Q1_monthly_demand_per_zone": QUERY_MONTHLY_DEMAND_PER_ZONE,
    "Q2_avg_distance_by_weather": QUERY_AVG_DISTANCE_BY_WEATHER,
    "Q3_air_quality_vs_demand": QUERY_AIR_QUALITY_VS_DEMAND,
    "Q4_zones_most_weather_sensitive": QUERY_ZONES_MOST_WEATHER_SENSITIVE,
    "Q5_peak_hour_per_day": QUERY_PEAK_HOUR_PER_DAY,
    "Q6_monthly_trends": QUERY_MONTHLY_TRENDS,
}