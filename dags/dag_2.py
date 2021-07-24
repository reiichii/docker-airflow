from datetime import datetime
from typing import Collection, Dict, Iterable, List
from airflow.models.xcom import XCom

from airflow.utils.dates import days_ago
from airflow.models import DAG, TaskInstance, Variable
from airflow.exceptions import AirflowException
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import BaseOperator, SkipMixin, TaskReschedule

DAG_ID = "dag_2"
DEFAULT_ARGS = {'start_date': datetime(2021, 7, 21) }


def register_date(ds, **kwargs):
    print(f'=== {ds} ===')


with DAG(DAG_ID,
        default_args=DEFAULT_ARGS,
        schedule_interval="0 10 * * *") as dag:
    t0 = DummyOperator(task_id='t0')

    t1 = PythonOperator(
        task_id='t1',
        python_callable=register_date,
        provide_context=True,
        retries=1,
    )

    t0 >> t1