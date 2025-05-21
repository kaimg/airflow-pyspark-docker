from datetime import datetime, timedelta
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from scripts.pyspark_pricebook_analytics_job import run_pricebook_analytics_pipeline

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pricebook_analytics_dag_python_operator',
    default_args=default_args,
    description='Run PySpark ETL job using PythonOperator',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['pyspark'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    run_spark_job = PythonOperator(
        task_id='run_pricebook_analytics_etl',
        python_callable=run_pricebook_analytics_pipeline,
        op_kwargs={},
    )

    (
        start 
        >> run_spark_job 
        >> end
    )