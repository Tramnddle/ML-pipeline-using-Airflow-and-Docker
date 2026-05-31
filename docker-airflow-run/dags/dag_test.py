import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from airflow import DAG
from airflow.operators.dummy import DummyOperator

from airflow.hooks.base_hook import BaseHook
from airflow.operators.slack_operator import SlackAPIPostOperator
import datetime

from src import config


DAG_ID = "slack_test_workflow"
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
        start = DummyOperator(task_id="start")
        try:
            slack_connection = BaseHook.get_connection(config.SLACK_CONNECTION_ID)
            task = SlackAPIPostOperator(
                task_id=f"_slack_message_",
                token=slack_connection.password,
                text="""Hello World! This is a test message from Airflow!""",
                channel=slack_connection.login,
                username="airflow",
            )
        except Exception:
            task = DummyOperator(task_id="_slack_message_")
        end = DummyOperator(task_id="end")
        start >> task >> end
        
    return dag

globals()[DAG_ID] = create_dag(DAG_ID)
