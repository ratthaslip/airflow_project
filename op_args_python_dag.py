from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def test_function(name,age):
    print(f"My name is {name},and i am {age} years old")

my_dag = DAG(
    'op_args_python_dag',
    start_date=datetime(2023, 2, 1),
    schedule_interval='@daily'
)

python_task = PythonOperator(
    task_id='python_task',
    python_callable=test_function,
    op_args=['jack', 32],
    dag=my_dag
)

python_task