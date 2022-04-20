from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint

def extract_transform():
    # TODO: Extract and transform data to standard format

def store_to_hdfs():
    hdfs = PyWebHdfsClient(host='10.121.101.145',
                           port='50070', user_name='cloudera')
    my_dir = '/user/cloudera/raw/index_dashboard/Global/CGI_4.0'
    hdfs.make_dir(my_dir)
    hdfs.make_dir(my_dir, permission=755)

    ingest_date = datetime.now().strftime("%Y%m%d%H%M%S")

    with open('/opt/airflow/dags/output/CGI_4.0_2017.csv', 'r', encoding="utf8") as file_data:
        my_data = file_data.read()
        hdfs.create_file(
            my_dir + '/CGI_4.0_2019_{}.csv'.format(ingest_date), my_data.encode('utf-8'), overwrite=True)

    pprint("Stored!")
    pprint(hdfs.list_dir(my_dir))

default_args = {
    'owner': 'ETDA',
    'start_date': datetime(2022, 3, 31),
    'schedule_interval': None,
}

dag = DAG('example_dag',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)

with dag:
    load_data_source = BashOperator(
        task_id='load_data_source',
        bash_command='cd /opt/airflow/dags/data_source &&  curl -LfO "https://www.teknologisk.dk/_/media/76459_GCR%2017-19%20Dataset.xlsx"',
    )

    extract_transform = PythonOperator(
        task_id='extract_transform',
        python_callable=extract_transform,
    )

    load_to_hdfs = PythonOperator(
        task_id='load_to_hdfs',
        python_callable=store_to_hdfs,
    )

load_data_source >> extract_transform >> load_to_hdfs