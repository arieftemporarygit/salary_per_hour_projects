from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'salary_per_hours_sql',
    default_args=default_args,
    description='A SQL salary_per_hours data pipeline',
    schedule_interval='30 21 * * *',
)

extract_salary = PythonOperator(
    task_id='extract_salary',
    python_callable=lambda: os.system('python /opt/airflow/scripts/sql/extract_salary.py'),
    dag=dag,
)

transform_salary = PythonOperator(
    task_id='transform_salary',
    python_callable=lambda: os.system('python /opt/airflow/scripts/sql/transform_salary.py'),
    dag=dag,
)

load_salary = PythonOperator(
    task_id='load_salary',
    python_callable=lambda: os.system('python /opt/airflow/scripts/sql/load_salary.py'),
    dag=dag,
)

extract_salary >> transform_salary >> load_salary
