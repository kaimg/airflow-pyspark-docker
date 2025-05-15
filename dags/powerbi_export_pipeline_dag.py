from datetime import datetime, timedelta
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.empty import EmptyOperator # type: ignore
from scripts.pyspark_powerbi_etl_job import run_contracts_pipeline

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='powerbi_contract_export_pipeline',
    default_args=default_args,
    description='ETL pipeline to export contract data to Power BI dashboard',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['powerbi', 'etl', 'contracts'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    run_contract_export = PythonOperator(
        task_id='run_contract_export',
        python_callable=run_contracts_pipeline,
        op_kwargs={},
    )

    (
        start 
        >> run_contract_export 
        >> end
    )