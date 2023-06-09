from datetime import timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from datetime import datetime

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 3, 4)
}

with DAG(
    dag_id='batch_fullload',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60)
) as dag:
    bash = BashOperator(
        task_id="start",
        bash_command="ls /opt/workspace/notebooks/",
    )

    notebook_raw_task = PapermillOperator(
        task_id="raw",
        input_nb="/opt/workspace/notebooks/batch_fullload/raw.ipynb",
        output_nb="/opt/workspace/notebooks/batch_fullload/executions/raw/out-{{ execution_date }}.ipynb",
        parameters={"execution_date": "{{ execution_date }}"},
    )

    notebook_cleaned_task = PapermillOperator(
        task_id="cleand",
        input_nb="/opt/workspace/notebooks/batch_fullload/cleaned.ipynb",
        output_nb="/opt/workspace/notebooks/batch_fullload/executions/cleaned/out-{{ execution_date }}.ipynb",
        parameters={"execution_date": "{{ execution_date }}"},
    )

    notebook_curated_task = PapermillOperator(
        task_id="curated",
        input_nb="/opt/workspace/notebooks/batch_fullload/curated.ipynb",
        output_nb="/opt/workspace/notebooks/batch_fullload/executions/curated/out-{{ execution_date }}.ipynb",
        parameters={"execution_date": "{{ execution_date }}"},
    )

    bash >> notebook_raw_task >> notebook_cleaned_task >> notebook_curated_task