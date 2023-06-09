from datetime import timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from datetime import datetime

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 6, 4)
}

with DAG(
    dag_id='cdc',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 * * * *',
    dagrun_timeout=timedelta(minutes=60)
) as dag:
    bash = BashOperator(
        task_id="start",
        bash_command="ls /opt/workspace/notebooks/",
    )

    notebook_raw_task = PapermillOperator(
        task_id="raw",
        input_nb="/opt/workspace/notebooks/cdc/raw.ipynb",
        output_nb="/opt/workspace/notebooks/cdc/executions/raw/out-{{ execution_date }}.ipynb",
        parameters={"execution_date": "{{ execution_date }}"},
    )

    notebook_cleaned_task = PapermillOperator(
        task_id="cleaned",
        input_nb="/opt/workspace/notebooks/cdc/cleaned.ipynb",
        output_nb="/opt/workspace/notebooks/cdc/executions/cleaned/out-{{ execution_date }}.ipynb",
        parameters={"execution_date": "{{ execution_date }}"},
    )

    bash >> notebook_raw_task >> notebook_cleaned_task