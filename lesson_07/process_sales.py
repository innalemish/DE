from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from datetime import datetime

def check_response(response):
    if response.status_code == 201:
        return True
    else:
        raise ValueError('Job failed with status code: {}'.format(response.status_code))

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 8, 9),
    'retries': 1,
}

with DAG(dag_id='process_sales',
         default_args=default_args,
         schedule_interval='0 1 * * *',
         catchup=True,
         max_active_runs=1) as dag:

    extract_data_from_api = SimpleHttpOperator(
        task_id='extract_data_from_api',
        method='GET',
        http_conn_id='api_connection',
        endpoint='path/to/your/api',
        headers={"Content-Type": "application/json"},
        response_check=check_response,
        params={'raw_dir': '/path/to/my_dir/raw/sales/{{ ds }}'},
        dag=dag
    )

    convert_to_avro = SimpleHttpOperator(
        task_id='convert_to_avro',
        method='GET',
        http_conn_id='api_connection',
        endpoint='path/to/your/api',
        headers={"Content-Type": "application/json"},
        response_check=check_response,
        params={'stg_dir': '/path/to/my_dir/stg/sales/{{ ds }}'},
        dag=dag
    )

    extract_data_from_api >> convert_to_avro