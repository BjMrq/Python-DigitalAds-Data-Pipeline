# load the dependencies
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import date, timedelta, datetime

import fetching_facebook_data as fetch
import cleanning_facebook_data as clean
import load_data_bigquery as load
import delete_csv as delete

DAG_DEFAULT_ARGS = {
	'owner': 'airflow',
	'depends_on_past': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=1)
}

with DAG('facebook_report', start_date=datetime(2019, 01, 1), schedule_interval="@daily", default_args=DAG_DEFAULT_ARGS, catchup=False) as dag:
    fetching_facebook_task = PythonOperator(task_id="fetching_facebook_data_task", python_callable=fetching_facebook_data.main)

    cleanning_facebook_data_task = PythonOperator(task_id="cleanning_facebook_data_data_task", python_callable=cleanning_facebook_data.main)

    load_data_bigquery_task = PythonOperator(task_id="load_data_bigquery_task", python_callable=load_data_bigquery.main)

    delete_csv = PythonOperator(task_id="delete_csv_task", python_callable=delete_csv.main)

    fetching_facebook_task >> cleanning_facebook_data_task >> load_data_bigquery_task >> delete_csv
