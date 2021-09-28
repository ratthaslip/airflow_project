import json
from datetime import datetime

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 7, 1),
    'provide_context': True  # to support task instance for XComms with kwargs['ti']
}

with DAG('covid19_get_data',
         schedule_interval='@daily',
         default_args=default_args,
         description='A simple data pipeline for COVID-19 report',
         catchup=True) as dag:

    t1 = SimpleHttpOperator(
        task_id='get_covid19_report_today',
        method='GET',
        http_conn_id='',
        endpoint='',
        headers={""},
        dag=dag
    )


    t1 
