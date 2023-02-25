from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


my_dag_id = "my_python_dag"

default_args = {
    'owner': 'proton',
    'depends_on_past': False,
    'retries': 10,
    'concurrency': 1
}

def hello():
    print("Hello Airflow Using a Python Operator!")

dag = DAG(
    dag_id=my_dag_id,
    default_args=default_args,
    start_date=datetime(2019, 6, 17),
    schedule_interval=timedelta(minutes=5)
)

python_task = PythonOperator(task_id='python-task',
                             python_callable=hello,
                             dag=dag)
							 
python_task
