from datetime import datetime, timedelta
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from scripts.generic.currency_rate_etl_job import run_currency_etl_job

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="currency_dag_python_operator",
    default_args=default_args,
    description="Run Currency ETL job using PythonOperator",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["currency"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    run_currency_job = PythonOperator(
        task_id="run_currency_etl",
        python_callable=run_currency_etl_job,
        op_kwargs={},
    )

    (start >> run_currency_job >> end)
