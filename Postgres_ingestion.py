from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
from airflow.models import Variable
import os
import pytz
import psycopg2
import pandas as pd
from psycopg2 import Error

tzInfo = pytz.timezone('Asia/Bangkok')
output_path = "/opt/airflow/dags/output/erp"
ingest_date = datetime.now(tz=tzInfo)

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('ERP',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)


def ingestion():
    try:
        # Connect to an existing database
        connection = psycopg2.connect(user="odoo",
                                      password="p@ssw0rd",
                                      host="192.168.45.158",
                                      port="5432",
                                      database="etp")

        # Create a cursor to perform database operations
        cursor = connection.cursor()
        # Print PostgreSQL details
        # print("PostgreSQL server information")
        # print(connection.get_dsn_parameters(), "\n")

        # Executing a SQL query
        cursor.execute("SELECT * FROM account_account")
        # Fetch result
        record = cursor.fetchall()
        cols = []
        for elt in cursor.description:
            cols.append(elt[0])

        df = pd.DataFrame(data=record, columns=cols)
        df.to_csv(f'{output_path}/account_{ingest_date.strftime("%Y%m%d")}.csv')
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
