import os
from pathlib import Path


# Resolve project paths relative to this file so the project can live inside any
# Airflow DAG subdirectory, not only /opt/airflow/dags.
BASE_DIR = Path(__file__).resolve().parents[1]

PATH_DIR_DATA = os.environ.get("ML_PIPELINE_DATA_DIR", str(BASE_DIR / "data"))
PATH_DIR_MODELS = os.environ.get("ML_PIPELINE_MODELS_DIR", str(BASE_DIR / "models"))
PATH_DIR_RESULTS = os.environ.get("ML_PIPELINE_RESULTS_DIR", str(BASE_DIR / "results"))
PATH_TO_CREDENTIALS = os.environ.get("ML_PIPELINE_CREDS_PATH", str(BASE_DIR / "Creds.json"))
PATH_TO_APP_SHELL = os.environ.get("ML_PIPELINE_APP_SHELL", str(BASE_DIR / "app.sh"))

PATH_DIR_RAW = os.path.join(PATH_DIR_DATA, "raw")
PATH_DIR_PREPROCESSED = os.path.join(PATH_DIR_DATA, "preprocessed")

SLACK_CONNECTION_ID = os.environ.get("ML_PIPELINE_SLACK_CONN_ID", "slack_connection")


def ensure_runtime_directories():
    """
    Create the writable project directories if they do not already exist.
    """
    for path in (PATH_DIR_DATA, PATH_DIR_RAW, PATH_DIR_PREPROCESSED, PATH_DIR_MODELS, PATH_DIR_RESULTS):
        os.makedirs(path, exist_ok=True)


ensure_runtime_directories()

RANDOM_SEED = 42
TEST_SPLIT_SIZE = 0.3
PROB_THRESHOLD = 0.5
SPLIT_METHOD = "time based"

# lowest acceptable difference between the performances of the same model on two different datasets
MODEL_DEGRADATION_THRESHOLD = 0.1
ASSOCIATION_DEGRADATION_THRESHOLD = 0.3

# lowest acceptable performance of either accuracy, precision, recall, f1 or auc depending on the classification usecase
MODEL_PERFORMANCE_THRESHOLD = 0.7 
MODEL_PERFORMANCE_METRIC = "auc"

IDENTIFIERS = ['loan_id', 'customer_id']
TARGET = 'loan_status'
DATETIME_VARS = ['application_time']
EXC_VARIABLES = [
    'application_time'
    ]
PURPOSE_ENCODING_METHOD = "weighted ranking" # choose from (ranking, one-hot, weighted ranking, relative ranking)
RESCALE_METHOD = "standardize" # choose from (standardize, minmax, None)
CAT_VARS = [
    'term', 
    'home_ownership', 
    'purpose',
    'years_in_current_job', 
    ]
NUM_VARS = [
    'current_loan_amount', 
    'credit_score', 
    'monthly_debt',
    'annual_income',
    'years_of_credit_history', 
    'months_since_last_delinquent', 
    'no_of_open_accounts',
    'current_credit_balance',
    'max_open_credit',
    'bankruptcies',
    'tax_liens', 
    'no_of_properties', 
    'no_of_cars',
    'no_of_children', 
    'no_of_credit_problems', 
    ]

PREDICTORS = [
    "current_loan_amount",
    "term",
    "credit_score",
    "years_in_current_job",
    "home_ownership",
    "annual_income",
    "purpose",
    "monthly_debt",
    "years_of_credit_history",
    "months_since_last_delinquent",
    "no_of_open_accounts",
    "no_of_credit_problems",
    "current_credit_balance",
    "max_open_credit",
    "bankruptcies",
    "tax_liens",
    'no_of_properties', 
    'no_of_cars',
    'no_of_children',
    "application_year",
    "application_month",
    "application_week",
    "application_day",
    "application_season",
    "current_credit_balance_ratio",
]

STAGES = [
    "etl", "preprocess", "training", "testing", "inference", "postprocess", "preprocess-training", "preprocess-inference", "report", "driftcheck",
    "etl_report", "raw_data_drift-report", "deploy"
    ]
STATUS = ["pass", "fail", "skipped", "started"]
JOB_TYPES = ["training", "inference", None]

