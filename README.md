# Data Engineering Platform



## Overview
This project implements a scalable data engineering platform using a multi-hop Delta Lake architecture. It is designed to ingest heterogeneous data formats, apply automated data quality quarantine rules, and establish a consistent common data model. The final integration layer enriches New York taxi trip data with historical weather and air quality metrics to support highly performant analytical queries.

## Prerequisites
Ensure your local environment has the following dependencies installed before executing the pipelines:
* Python 3.9 - 3.12
* Required Python packages installed via `requirements.txt`

## Project Structure
The repository is organized to cleanly separate configuration, source code, data storage, and generated analytical reports.
* `config/`: Configuration files defining generic ingestion rules, schema mappings, and partitioning logic.
* `src/`: Core Python modules for tasks including ingestion (Bronze/Silver), integration (Gold), and benchmarking.
* `data/raw`: Raw data files before processing.
* `data/delta`: Processed data in Delta format.
* `notebooks/`: Jupyter notebooks for exploratory data analysis, ingestion, integration, and benchmarking.
* `reports/`: Output directory for generated assets.

## Execution Steps
Run the platform in the following order to reproduce the results:
* Create a virtual environment: `python -m venv venv` (python versions 3.9 - 3.12)
* Activate the virtual environment:
  * Windows: `venv\Scripts\activate`
  * macOS/Linux: `source venv/bin/activate`

* Initialize the environment dependencies: `pip install -r requirements.txt`

## Week 1: Build a Generic Urban Data Integration Platform

* Load sample data as follows (ensure the unzipped data is placed in the correct directories):
    * Air Quality Data: `data/raw/air_quality/`
    * Taxi Zone Lookup Data: `data/raw/taxi_zone_lookup/`
    * Taxi Trip Data: `data/raw/tripdata/`
    * Weather Data: `data/raw/weather/`
* Open the Jupyter notebook for exploratory analysis: `jupyter notebook`
* Execute data ingestion and quarantine rules (Tasks 1-3): `notebooks/week1/run_ingestion.ipynb`
* Execute the integration layer to enrich the data and generate analytical reports (Tasks 4-5): `notebooks/week1/run_integration.ipynb`
* Generate storage strategy metrics and query latency reports (Task 6): `notebooks/week1/run_benchmark.ipynb`

## Week 2: Querying and Optimizing the Urban Data Platform

* Week 2 extends the platform by implementing reusable analytical SQL queries, evaluating Spark query optimizations, and materializing automated analytical data products.
* Design of analytical queries (Task 1): `notebooks/week2/run_analytics.ipynb`
* Optimisation and Benchmarks (Tasks 2,3 and 5): `notebooks/week2/optimize_query_&_benchmarking.ipynb`
* Materialization of Analytical Data Products (Task 4): `notebooks/week2/run_reusable_products.ipynb`
