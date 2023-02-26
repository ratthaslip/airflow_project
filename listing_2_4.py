  
import json
import pathlib


import requests
import requests.exceptions as requests_exceptions

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

output_path = "/opt/airflow/dags/output"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG(
    dag_id="listing_2_4",
    description="Download rocket pictures of recently launched rockets.",
    default_args=default_args)


download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o {}/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'".format(output_path),  # noqa: E501
    dag=dag
)

download_launches