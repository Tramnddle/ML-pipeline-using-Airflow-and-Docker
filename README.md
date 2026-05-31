# Loan Prediction

## 1. Environment Setup
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
