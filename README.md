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

## Week 3: Operating and Maintaining the Urban Data Platform

* Week 3 expands the platform by handling incremental data updates, keeping the analytical data products consistent under schema evolution, monitoring every pipeline run, extending the validation framework, and evaluating the production readiness of the platform.
* Run the notebooks in the order below; Week 1 ingestion and integration and the Week 2 data products must have been run first.
* Generation and ingestion of incremental update datasets (Task 1): `notebooks/week3/generate_incremental_updates.ipynb`
* Analytical consistency and platform monitoring (Tasks 2 and 3): `notebooks/week3/maintain_analytical_consistency.ipynb`
* Extended data validation framework (Task 4): `notebooks/week3/expanded_ingestion.ipynb`, with validation rules and schema policies in `config/datasets-expanded.yaml`
* Evaluation of the platform (Task 5): `notebooks/week3/evaluate_platform.ipynb`, results written to `reports/week3_evaluation/` 

## Week 4: Building Machine Learning Pipelines on the Urban Data Platform

* Week 4 uses the platform for predictive analytics: it predicts hourly taxi demand per pickup zone with a Spark MLlib pipeline (gradient-boosted trees), built from the integrated Gold table and driven by `config/ml.yaml`.
* Run the notebooks in the order below; Week 1 ingestion and integration must have been run first.
* Generation of the training dataset (Task 1): `notebooks/week4/tran_dataset.ipynb`. Writes the Delta table `data/delta/ml/training_dataset` (train/validation/test splits) and the manifest `reports/week4/training_dataset_manifest.json`.
* Reusable feature engineering pipeline (Task 2): `notebooks/week4/feature_pipeline.ipynb`. Writes the feature dataset to `data/ml/feature_dataset` and the fitted pipeline to `models/feature_pipeline`. Features, missing-value rules and scaling are set in the `features` section of `config/ml.yaml`.
* Training and evaluation of the model (Task 3): `notebooks/week4/ml_pipeline.ipynb`. Each run is saved under `models/demand_model/<run_id>/` (pipeline + `run.json` with metrics, timings and reproducibility fingerprints); the latest run is recorded in `models/demand_model/latest.json` and its metrics in `reports/week4/model_metrics.json`. The algorithm and its parameters are set in the `model` section of `config/ml.yaml`.
* Comparison of the raw-data and platform approaches (Task 4): `notebooks/week4/task4_approach_comparison.ipynb`, results written to `reports/week4/comparison/`. Approach A also needs the original raw files in `data/raw/`; outputs go to separate `comparison` folders, so the main Week 4 artifacts are not overwritten.