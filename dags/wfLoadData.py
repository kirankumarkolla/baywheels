from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys
sys.path.append('/usr/local/airflow/')
from src import downloadFiles,loadToStg


default_args={
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    'email': ['kiran.kolla@toptal.com'],
    'email_on_failure': True,
    "start_date": datetime(2021, 1, 1)
    }

dag = DAG(dag_id="load",schedule_interval=None,default_args=default_args,catchup=False)   

  
download_task = PythonOperator(task_id="download",python_callable=downloadFiles.downloadFiles,dag=dag)
stgload_task = PythonOperator(task_id="stgload",python_callable=loadToStg.loadStaging,dag=dag)
clnload_task = PythonOperator(task_id="clnload",python_callable=loadToStg.dataCleansing,dag=dag)
dimload_task = PythonOperator(task_id="dimload",python_callable=loadToStg.stations_dim_load,dag=dag)

download_task >> stgload_task >> clnload_task >> dimload_task