# Loan Prediction

<<<<<<< Updated upstream
=======
# Pre-requisites

- Basic experience with training a machine learning model using scikit-learn or xgboost
- Basic experience with serving a pretrained using flask
- Basic familiarity with Postgressql
- Experience with docker and docker-compose
- Basic statistics on hypothesis testing

### Note: This project requires the data to be present in postgres server.

The data is available in code>main>dags>data>raw location.

Kindly upload the data and provide the appropriate credentials in code>main>dags>creds.json file.

## Running inside an existing Airflow deployment

This repository ships with its own example `docker-compose.yaml`, but the DAG
code can also be mounted into an already-running Airflow instance.

### What changed in this repo

- Paths are now resolved relative to the project folder instead of assuming `/opt/airflow/dags`
- Source Postgres credentials can come from environment variables or `Creds.json`
- Slack notification tasks degrade to no-op tasks when the Slack Airflow connection is absent

### Recommended layout in your existing Airflow `dags/` folder

```text
dags/
  ml_pipeline_monitoring/
    dag_pipeline.py
    dag_test.py
    app.py
    src/
    data/
    models/
    results/
    Creds.json
```

### Environment variables for the source Postgres database

If you do not want to keep credentials in `Creds.json`, set these variables in
your existing Airflow deployment:

- `ML_PIPELINE_DB_HOST`
- `ML_PIPELINE_DB_PORT`
- `ML_PIPELINE_DB_NAME`
- `ML_PIPELINE_DB_USER`
- `ML_PIPELINE_DB_PASSWORD`

Optional overrides:

- `ML_PIPELINE_DATA_DIR`
- `ML_PIPELINE_MODELS_DIR`
- `ML_PIPELINE_RESULTS_DIR`
- `ML_PIPELINE_CREDS_PATH`
- `ML_PIPELINE_SLACK_CONN_ID`

### Notes

- Keep the whole project together under one subfolder so imports like `src.*` continue to work
- Keep `dag_id` values unique across your Airflow deployment
- Install this project's Python dependencies into your existing Airflow image or environment

## 1. What's new?

>>>>>>> Stashed changes
ML pipeline monitoring using:

- Deepcheks
- Airflow
- Slack integration: alerts

## 2. Environment Setup
## Environment Setup and Deployment

The project environment was containerized using Docker and Docker Compose to ensure a reproducible and platform-independent deployment process.

The source code was maintained in a GitHub repository and cloned to the local development environment. Prior to deployment, the configuration file (`dags/src/config.py`) was adjusted by setting the parameter `RUN_LOCAL=False`, allowing the application to operate within the containerized environment rather than relying on local execution.

To support the required services, Docker was configured with a minimum allocation of 4 GB of memory. The complete application stack was then deployed using Docker Compose from the project root directory.

The deployment automatically provisioned and orchestrated all required services, including:

* Apache Airflow Webserver
* Apache Airflow Scheduler
* Apache Airflow Worker
* Apache Airflow Triggerer
* Flower Monitoring Service
* PostgreSQL Database
* Redis Message Broker

After startup, container health was verified using Docker status checks to ensure that all services were running correctly and communicating with each other. The resulting environment provided a fully operational workflow orchestration platform with Airflow, backed by PostgreSQL for metadata storage and Redis as the message broker for task execution.

This containerized setup enabled consistent development, testing, and deployment across different environments while simplifying dependency management and service orchestration.

## 3. How to reset environments

1. Delete all files under the following subdirectories. In case subdirectories do not exist (due to .gitignore) please create them

   - `dags/data/raw/*`
   - `dags/data/preprocessed`
   - `dags/models`
   - `dags/results`

   At the end, the directory should be structured as following (ensure to manually create any directory that is missing)

   ```
       ├── airflow.sh
       ├── dags
       │   ├── app.py
       │   ├── credentials.json
       │   ├── dag_pipeline.py
       │   ├── dag_training.py
       │   ├── data
       │   │   ├── preprocessed
       │   │   │   ├── 
       │   │   └── raw
       │   │       ├── 
       │   ├── main.py
       │   ├── models
       │   │   ├── deploy_report.json
       │   ├── results
       │   │   ├── 
       │   ├── src
       │   │   ├── config.py
       │   │   ├── drifts.py
       │   │   ├── etl.py
       │   │   ├── helpers.py
       │   │   ├── inference.py
       │   │   ├── preprocess.py
       │   │   ├── queries.py
       │   │   └── train.py
       ├── docker-compose.yaml
       ├── jobs
       ├── logs
       │   ├── 
       ├── plugins
       ├── readme.md
       └── requirements.txt
   ```
2. Truncate the `mljob` table

   - `truncate mljob;`

# Monitoring Machine Learning Pipeline

## 1. Traditional machine learning model training pipeline

1. data gathering
2. data preprocessing
3. model training
4. model evaluation
5. model serving

## 2. The idea behind model training monitoring

1. data integrity
2. data drift
3. concept drift
4. comparative analysis of models
