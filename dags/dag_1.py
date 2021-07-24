import random
from datetime import datetime
from textwrap import dedent
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


DAG_ID = "dag_1"
DEFAULT_ARGS = {'start_date': datetime(2021, 7, 21) }


def register_date(ds, **kwargs):
    print(f'=== {ds} ===')
    Variable.set(ds, random.randint(1,10), serialize_json=True)


templated_command = dedent(
    """
    {% for i in range(3) %}
        echo "Execution date is {{ ds }}"
        echo "My name is {{ params.name }}"
    {% endfor %}
    """
)

with DAG(DAG_ID,
        default_args=DEFAULT_ARGS,
        schedule_interval="0 5 * * *") as dag:
    t0 = DummyOperator(task_id='t0')

    t1 = PythonOperator(
        task_id='t1',
        python_callable=register_date,
        provide_context=True,
        retries=1,
    )

    t2 = BashOperator(
        task_id='t2',
        bash_command=templated_command,
        params={'name': 'taro'}
    )

    t0 >> [t1, t2]
