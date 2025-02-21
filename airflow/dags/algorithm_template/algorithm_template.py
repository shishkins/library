from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from src.airflow_defaults import tags, users
from src.airflow_defaults.users import get_emails
from src.airflow_defaults.utils import AirflowDAG

from algorithm_template.src import tasks


with AirflowDAG(
        dag_id='algorithm_template',
        description='Шаблон алгоритма/DAG-а',
        schedule_interval='0 9 * * 1', # https://crontab.guru
        owner=users.tretyakov.name,  # владелец/разработчик дага. Если не указан, удаляйте этот аргумент
        retry_delay=timedelta(minutes=1),
        catchup=False,
        telegram_on_failure=True,
        dag_file=__file__,
        tags=[
            tags.Period.weekly,
        ]
) as dag:

    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=tasks.task_1,
    )