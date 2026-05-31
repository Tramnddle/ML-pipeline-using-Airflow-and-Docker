import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.slack_operator import SlackAPIPostOperator

import traceback
import datetime
import numpy as np


from src import config
from src import helpers
from src import etl
from src import preprocess
from src import train
from src import drifts

TARGET = config.TARGET
PREDICTORS = config.PREDICTORS
DAG_ID = "ml_pipeline_monitoring"


def build_notification_task(dag_id: str, suffix: str, text: str):
    """
    Create a Slack notification task when the configured Airflow connection
    exists, otherwise fall back to a no-op task so the DAG still imports.
    """
    try:
        slack_connection = BaseHook.get_connection(config.SLACK_CONNECTION_ID)
        return SlackAPIPostOperator(
            task_id=f"_{dag_id}__{suffix}_",
            token=slack_connection.password,
            text=text,
            channel=slack_connection.login,
            username="airflow",
        )
    except Exception:
        return DummyOperator(task_id=f"_{dag_id}__{suffix}_")

def init(ti, job_id:str, **context):
    """
    Initialize the job_id and the start_date and end_date.
    :param job_id: the job_id of the current job
    """
    start_date = context['dag_run'].conf.get('start_date')
    end_date = context['dag_run'].conf.get('end_date')
    last_deployed_job_id = helpers.get_latest_deployed_job_id()
    ti.xcom_push(key='job_id', value=job_id)
    ti.xcom_push(key='last_deployed_job_id', value=last_deployed_job_id)
    
    if end_date is None:
        end_date = datetime.date.today()
    else:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    
    if start_date is None:
        # start_date = end_date - datetime.timedelta(days=30*6)
        start_date = datetime.date(2015, 6, 1)
    else:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    assert isinstance(start_date, datetime.date)
    assert isinstance(end_date, datetime.date)
    assert start_date < end_date, "start_date must be less than end_date"
    
    ti.xcom_push(key='end_date', value=str(end_date))
    ti.xcom_push(key='start_date', value=str(start_date))
    
def get_data(ti, mode:str="training"):
    """
    Extract data from the database and save it to the raw data directory.
    :param mode: the job type of the data extraction. Either "training" or "inference"
    """
    job_id = ti.xcom_pull(key='job_id', task_ids=f'_{DAG_ID}__init_')
    start_date = ti.xcom_pull(key='start_date', task_ids=f'_{DAG_ID}__init_')
    end_date = ti.xcom_pull(key='end_date', task_ids=f'_{DAG_ID}__init_')
    helpers.create_table_ml_job()
    helpers.log_activity(job_id, job_type=mode, stage="etl", status="started", message="", job_date=end_date)
    try:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        assert start_date < end_date, "start_date must be less than end_date"
        etl.collect_data(start_date=start_date, end_date=end_date, job_id=job_id)
        helpers.log_activity(job_id, job_type=mode, stage="etl", status="pass", message="", job_date=end_date)
    except Exception as e:
        ti.xcom_push(key='exception', value=str(e))
        message = str(traceback.format_exc())
        helpers.log_activity(job_id, job_type=mode, stage="etl", status="fail", message=message, job_date=end_date)
        raise(Exception(message))

def check_data_quality(ti):
    """
    Check the quality of the data.
    """
    job_id = ti.xcom_pull(key='job_id', task_ids=f'_{DAG_ID}__init_')
    curr_df = helpers.load_dataset(helpers.locate_raw_data_filename(job_id))
    train = drifts.check_data_quality(curr_df, PREDICTORS, TARGET, job_id)
    if train["retrain"]:
        return f'_{DAG_ID}__check_data_drift_'
    else:
        return f'_{DAG_ID}__slack_data_quality_'

def check_data_drift(ti):
    """
    Check for any drift in the data.
    """
    job_id = ti.xcom_pull(key='job_id', task_ids=f'_{DAG_ID}__init_')
    last_deployed_job_id = ti.xcom_pull(key='last_deployed_job_id', task_ids=f'_{DAG_ID}__init_')
    if last_deployed_job_id is None:
        ti.xcom_push(key='job_id', value=job_id)
        return f'_{DAG_ID}__preprocess_'
    last_df = helpers.load_dataset(helpers.locate_raw_data_filename(last_deployed_job_id))
    curr_df = helpers.load_dataset(helpers.locate_raw_data_filename(job_id))
    retrain = drifts.check_data_drift(last_df, curr_df, PREDICTORS, TARGET, job_id)
    if retrain["retrain"]:
        return f'_{DAG_ID}__preprocess_'
    else:
        return f'_{DAG_ID}__slack_data_drift_'

def preprocess_data(ti, mode:str=None):
    """
    Preprocess the data for retraining.
    :param mode: the job type of the data extraction. Either "training" or "inference"
    """

    job_id = ti.xcom_pull(key='job_id', task_ids=f'_{DAG_ID}__init_')
    last_deployed_job_id = ti.xcom_pull(key='last_deployed_job_id', task_ids=f'_{DAG_ID}__init_')
    date = ti.xcom_pull(key='end_date', task_ids=f'_{DAG_ID}__init_')
    helpers.create_table_ml_job()
    helpers.log_activity(job_id, job_type=mode, stage="preprocess", status="started", message="", job_date=date)
    try:
        curr_df = helpers.load_dataset(helpers.locate_raw_data_filename(job_id))
        if last_deployed_job_id is None:
            preprocess.preprocess_data(df=curr_df, mode="training", job_id=job_id)
        else:
            preprocess.preprocess_data(df=curr_df, mode=mode, job_id=job_id, ref_job_id=last_deployed_job_id)
        ti.xcom_push(key='job_id', value=job_id)
        ti.xcom_push(key='last_deployed_job_id', value=last_deployed_job_id)
        helpers.log_activity(job_id, job_type=mode, stage="preprocess", status="pass", message="", job_date=date)
    except Exception as e:
        ti.xcom_push(key='exception', value=str(e))
        message = str(traceback.format_exc())
        helpers.log_activity(job_id, job_type=mode, stage="preprocess", status="fail", message=message, job_date=date)
        raise(Exception(message))
    
def check_model_drift(ti):
    """
    Check for any model drift.
    """
    job_id = ti.xcom_pull(key='job_id', task_ids=f'_{DAG_ID}__init_')
    last_deployed_job_id = ti.xcom_pull(key='last_deployed_job_id', task_ids=f'_{DAG_ID}__init_')
    if last_deployed_job_id is None:
        return f'_{DAG_ID}__train_'
    _, last_filename = helpers.locate_preprocessed_filenames(last_deployed_job_id)
    _, curr_filename = helpers.locate_preprocessed_filenames(job_id)
    model_type = helpers.get_model_type(last_deployed_job_id)
    try:
        model = helpers.load_model_from_pickle(f"{last_deployed_job_id}_{model_type}")
    except Exception as exc:
        if helpers.is_incompatible_sklearn_pickle_error(exc):
            print(
                f"[WARNING] Deployed baseline model `{last_deployed_job_id}_{model_type}` "
                "is incompatible with the current scikit-learn version. "
                "Running model drift in the legacy scikit-learn runtime."
            )
            retrain = helpers.run_legacy_model_drift(
                ref_csv_path=last_filename,
                cur_csv_path=curr_filename,
                model_name_or_path=f"{last_deployed_job_id}_{model_type}",
                predictors=PREDICTORS,
                target=TARGET,
                job_id=job_id,
            )
            if retrain["retrain"]:
                return f'_{DAG_ID}__train_'
            return f'_{DAG_ID}__slack_model_drift_'
        raise
    retrain = drifts.check_model_drift(
        ref_df=helpers.load_dataset(last_filename), 
        cur_df=helpers.load_dataset(curr_filename), 
        model=model, 
        predictors=PREDICTORS,
        target=TARGET, 
        job_id=job_id
        )
    if retrain["retrain"]:
        return f'_{DAG_ID}__train_'
    else:
        return f'_{DAG_ID}__slack_model_drift_'

def train_model(ti, mode:str="training"):
    """
    Train the model.
    :param mode: the job type of the data extraction. Either "training" or "inference"
    """
    job_id = ti.xcom_pull(key='job_id', task_ids=f"_{DAG_ID}__init_")
    date = ti.xcom_pull(key='end_date', task_ids=f"_{DAG_ID}__init_")
    helpers.create_table_ml_job()
    helpers.log_activity(job_id, job_type=mode, stage="training", status="started", message="", job_date=date)
    try:
        train_filename, test_filename = helpers.locate_preprocessed_filenames(job_id)
        train.train(train_dataset_filename=train_filename, test_dataset_filename=test_filename, job_id=job_id)
        helpers.log_activity(job_id, job_type=mode, stage="training", status="pass", message="", job_date=date)
    except Exception as e:
        ti.xcom_push(key='exception', value=str(e))
        message = str(traceback.format_exc())
        helpers.log_activity(job_id, job_type=mode, stage="training", status="fail", message=message, job_date=date)
        raise(Exception(message))

def deploy_model(ti, metric:str):
    """
    Compare the current model and the last model on the current test dataset and deploy the best performing model.
    :param metric: The metric to be used for evaluating the model.
    """
    job_id = ti.xcom_pull(key='job_id', task_ids=f'_{DAG_ID}__init_')
    date = ti.xcom_pull(key='end_date', task_ids=f'_{DAG_ID}__init_')
    helpers.create_table_ml_job()
    helpers.log_activity(job_id, job_type="training", stage="deploy", status="started", message="", job_date=date)
    try:
        print("[DEBUG] job_id:", job_id)
        model_type = helpers.get_model_type(job_id)
        model_name = f"{job_id}_{model_type}"
        model = helpers.load_model_from_pickle(model_name)
        _, test_filename = helpers.locate_preprocessed_filenames(job_id)
        df = helpers.load_dataset(test_filename)

        last_deployed_job_id = ti.xcom_pull(key='last_deployed_job_id', task_ids=f'_{DAG_ID}__init_')
        if last_deployed_job_id is None:
            # deploy current model
            train.pick_model_and_deploy(job_id, models=[{"model_name": model_name, "model":model}], df=df, metric=metric, predictors=PREDICTORS, target=TARGET)
            helpers.log_activity(job_id, "training", "deploy", "pass", "", date)
            return
        
        last_model_type = helpers.get_model_type(last_deployed_job_id)
        last_model_name = f"{last_deployed_job_id}_{last_model_type}"
        try:
            last_model = helpers.load_model_from_pickle(last_model_name)
            models = [
                {"model_name": last_model_name, "model": last_model},
                {"model_name": model_name, "model": model},
            ]
            train.pick_model_and_deploy(job_id, models=models, df=df, metric=metric, predictors=PREDICTORS, target=TARGET)
        except Exception as exc:
            if helpers.is_incompatible_sklearn_pickle_error(exc):
                print(
                    f"[WARNING] Deployed baseline model `{last_model_name}` is incompatible "
                    "with the current scikit-learn version. Evaluating it in the legacy runtime."
                )
                legacy_output = helpers.predict_with_legacy_model(
                    model_name_or_path=last_model_name,
                    dataset_path=test_filename,
                    predictors=PREDICTORS,
                )
                y_true = df[TARGET]
                current_pred = model.predict(df[PREDICTORS])
                current_prob = model.predict_proba(df[PREDICTORS])[:, 1]
                current_report = train.performance_report(y_true, current_pred, current_prob)
                legacy_report = train.performance_report(
                    y_true,
                    np.array(legacy_output["predictions"]),
                    np.array(legacy_output["probabilities"]),
                )
                chosen_model_name = (
                    last_model_name if legacy_report[metric] > current_report[metric] else model_name
                )
                helpers.persist_deploy_report(job_id, chosen_model_name)
                helpers.log_activity(job_id, "training", "deploy", "pass", "", date)
                return
            else:
                raise
        
        helpers.log_activity(job_id, "training", "deploy", "pass", "", date)
    except Exception as e:
        ti.xcom_push(key='exception', value=str(e))
        message = str(traceback.format_exc())
        helpers.log_activity(job_id, "training", "deploy", "fail", message, date)
        raise(Exception(message))
    
def create_dag(dag_id):
    with DAG(
        dag_id=dag_id,
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 0,
            "retry_delay": datetime.timedelta(minutes=1),
            "depends_on_past": False,
            "start_date": datetime.datetime.now() - datetime.timedelta(days=1)
        },
        catchup=False
        
    ) as dag:
        curr_job_id = helpers.generate_uuid()

        task_start = PythonOperator(task_id=f"_{dag_id}__init_", python_callable=init, op_kwargs={"job_id": curr_job_id})
        task_get_data = PythonOperator(task_id=f"_{dag_id}__get_data_", python_callable=get_data)
        task_check_data_quality = BranchPythonOperator(task_id=f"_{dag_id}__check_data_quality_", python_callable=check_data_quality)
        task_check_data_drift = BranchPythonOperator(task_id=f"_{dag_id}__check_data_drift_", python_callable=check_data_drift, provide_context=True)
        task_preprocess = PythonOperator(task_id=f"_{dag_id}__preprocess_", python_callable=preprocess_data, op_kwargs={"mode": "training"}, provide_context=True)
        task_check_model_drift = BranchPythonOperator(task_id=f"_{dag_id}__check_model_drift_", python_callable=check_model_drift, provide_context=True)
        task_train_model = PythonOperator(task_id=f"_{dag_id}__train_", python_callable=train_model, provide_context=True)
        task_deploy_model = PythonOperator(task_id=f"_{dag_id}__deploy_", python_callable=deploy_model, op_kwargs={"metric": "auc"})
        task_end = DummyOperator(task_id=f"_{dag_id}__end_")
        
        slack_data_quality = build_notification_task(
            dag_id,
            "slack_data_quality",
            ":red_circle: Training aborted due to data quality issues",
        )
        slack_data_drift = build_notification_task(
            dag_id,
            "slack_data_drift",
            ":white_circle: Training aborted since no data drift was detected",
        )
        slack_model_drift = build_notification_task(
            dag_id,
            "slack_model_drift",
            ":white_circle: No new model deployed since no model drift was detected",
        )
        task_start >> task_get_data >> task_check_data_quality >> task_check_data_drift >> task_preprocess >> task_check_model_drift >> task_train_model >> task_deploy_model >> task_end
        task_check_data_quality >> slack_data_quality
        task_check_data_drift >> slack_data_drift
        task_check_model_drift >> slack_model_drift
        
    return dag

globals()[DAG_ID] = create_dag(DAG_ID)
