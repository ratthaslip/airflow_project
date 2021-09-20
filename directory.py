rom airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta


default_dag_args = {
     'owner': 'airflow',
    'start_date': datetime(2021, 9, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
        ‘Create test_dir’,
         default_args=default_dag_args)


t1 = BashOperator(
	task_id=’Make directory’, bash_command=’mkdir  ’, dag=dag)


t2 = BashOperator(
	task_id=’Show directory’, bash_command=’ ’, dag=dag)

t1 >> t2   # This is how we set dependency among two tasks
