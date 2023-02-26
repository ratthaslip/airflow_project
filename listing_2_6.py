  
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
    dag_id="listing_2_6",
    description="Download rocket pictures of recently launched rockets.",
    default_args=default_args)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o {}/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'".format(output_path),  # noqa: E501
    dag=dag
)


def _get_pictures():
    # Ensure directory exists
    pathlib.Path("{}/images".format(output_path)).mkdir(parents=True, exist_ok=True)

    # Download all pictures in launches.json
    with open("{}/launches.json".format(output_path)) as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"{output_path}/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

get_pictures = PythonOperator(
    task_id="get_pictures", python_callable=_get_pictures, dag=dag
)

download_launches >> get_pictures
