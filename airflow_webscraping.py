import csv
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup

# # Define cvs file path
csv_path = "/opt/airflow/dags/output"

# # Define URL where scrape from.
url = "https://stackpython.co/courses"

# Default args used when create a new dag
default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 7, 18),
    # 'end_date': datetime(2018, 12, 30),
    "depends_on_past": False,
    # 'email': ['kohei.suzuki808@gmail.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "schedule_interval": "@once",
}


# Create a new dag
dag = DAG(
    "webscraping3",
    default_args=default_args,
    description="airflow webscraping",
    # Continue to run DAG once per day
    schedule_interval=timedelta(days=1),
    # schedule=timedelta(days=1),
)


# # Define task1
echo_start = BashOperator(
    task_id="echo_start",
    bash_command="echo Start scraping.",
    dag=dag,
)


def get_course_list(url):
    res = requests.get(url)
    res.encoding = "utf-8"
    soup = BeautifulSoup(res.text, "html.parser")

    datetime_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    courses = soup.find_all("h2")
    course_list = []
    for course in courses:

        # Create new variable --> obj to store
        # only course name getting rid of unwanted tags
        obj = course.string

        # Append each course into a course_list variable
        course_list.append(obj)
    course_list = [x.strip() for x in course_list if x is not None]
    return course_list


# # Define task2
scraping = PythonOperator(
    task_id="scraping",
    python_callable=get_course_list,
    op_kwargs={"url": url},
    dag=dag,
)


def write_csv(**kwargs):
    """
    Write information in the given list into a csv file.
    To get the list we just created, we use Xcoms.

    Returns
    -------
    Boolean: True or False

    When we success writing things correctly, returns True. Otherwise False.
    """

    # Xcoms to get the list
    ti = kwargs["ti"]
    course_list = ti.xcom_pull(task_ids="scraping")
    try:
        print(csv_path)
        with open("{}/test_file.csv".format(csv_path), "w") as file:
            writer = csv.writer(file, lineterminator="\n")
            writer.writerow(["title"])
            for row in course_list:
                writer.writerow([row])
        return True
    except OSError as e:
        print(e)
        return False


writing_csv = PythonOperator(task_id="writing_csv", python_callable=write_csv, dag=dag)


def confirmation(**kwargs):
    """
    If everything is done properly, print "Done!!!!!!"
    Otherwise print "Failed."
    """

    # Xcoms to get status which is the return value of write_csv().
    ti = kwargs["ti"]
    status = ti.xcom_pull(task_ids="writing_csv")

    if status:
        print("Done!!!!!!")
    else:
        print("Failed.")


confirmation = PythonOperator(
    task_id="confirmation", python_callable=confirmation, dag=dag
)


echo_start >> scraping >> writing_csv >> confirmation
