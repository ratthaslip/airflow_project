from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils import timezone
from airflow.decorators import task

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": timezone.datetime(2023, 1, 16),
    'schedule_interval': None,
}

with DAG(
    dag_id="a_example_xcom",
    default_args=DEFAULT_ARGS,
    tags=["test_xcom"],
):

    start = DummyOperator(task_id="start")

    @task
    def push_by_returning(ti=None):
        value_1 = {"a": "b"}
        return value_1

    @task
    def push(ti=None):
        value_2 = [1, 2, 3]
        ti.xcom_push(
            key="push_key", 
            value=value_2
        )

    @task
    def pull_data_from_xcom(ti=None):
        pulled_value_1 = ti.xcom_pull(
            task_ids="push_by_returning"
        )

        pulled_value_2 = ti.xcom_pull(
            task_ids="push", key="push_key"
        )

        pulled_value_3 = ti.xcom_pull(
            task_ids="push",
        )
        print(f"pulled_value_1 : {pulled_value_1}")
        print(f"pulled_value_2 : {pulled_value_2}")
        print(f"pulled_value_3 : {pulled_value_3}")

    end = DummyOperator(task_id="end")

    push_task = push()
    push_by_returning_task = push_by_returning()

    pull_data_from_xcom_task = pull_data_from_xcom()

    start >> [push_task, push_by_returning_task] >> pull_data_from_xcom_task >> end
