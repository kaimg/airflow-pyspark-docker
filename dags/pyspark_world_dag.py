from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pyspark_world_dag',
    default_args=default_args,
    description='Run PySpark ETL job using spark-submit',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['pyspark'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    run_spark_job = BashOperator(
        task_id='run_pyspark_etl',
        bash_command=(
            'spark-submit --jars /opt/airflow/postgresql-42.2.5.jar '
            '/opt/airflow/scripts/pyspark_etl_job.py'
        )
    )

    (
        start 
        >> run_spark_job 
        >> end
    )
