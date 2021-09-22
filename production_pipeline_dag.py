from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
from pprint import pprint

# Update the default arguments and apply them to the DAG.
default_dag_args = {
    'owner': 'airflow',
    'start_date': datetime(2019,1,1),
    'sla': timedelta(minutes=90),
}
    
dag = DAG('production_pipeline_dag', default_args=default_dag_args)



def process_data(**kwargs):
    pprint(kwargs)
    return 'processing data'

run_processing = PythonOperator(task_id='run_processing', 
                             python_callable=process_data,
                             provide_context=True,
                             dag=dag)

email_subject="""
  Email report for {{ params.department }} on {{ ds_nodash }}
"""

email_report_task=EmailOperator(task_id='email_report_task',
              to='your.email@gmail.com',
              subject=email_subject,
              html_content='',
              params={'department':'Data subscription services'},
              dag=dag)

no_email_task = DummyOperator(task_id='no_email_task', dag=dag)

def check_weekend(**kwargs):
    dt = datetime.strptime(kwargs['execution_date'],'%d-%m-%Y')
    #If dt.weekday() is 0-4, it's Mon-Fri. If 5-6, it's Sat/Sun
    pprint(dt.weekday())
    if (dt.weekday() < 5):
        return 'email_report_task'
    else:
        return 'no_email_task'
    
check_if_weekend = BranchPythonOperator(task_id='check_if_weekend',
                                   python_callable=check_weekend,
                                   provide_context=True,
                                   dag=dag)

run_processing >> check_if_weekend >> [email_report_task, no_email_task]
