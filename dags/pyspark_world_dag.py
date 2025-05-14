from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from scripts.pyspark_etl_job import run_pipeline

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pyspark_world_dag_python_operator',
    default_args=default_args,
    description='Run PySpark ETL job using PythonOperator',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['pyspark'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    run_spark_job = PythonOperator(
        task_id='run_pyspark_etl',
        python_callable=run_pipeline,
        op_kwargs={
            "source_table": "sales",
            "target_table": "sales_transformed",
            "write_mode": "overwrite"
        },
    )

    (
        start 
        >> run_spark_job 
        >> end
    )