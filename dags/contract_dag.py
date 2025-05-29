from datetime import datetime, timedelta
from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="contract_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for Contracts, Pricebooks, Requests",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["pyspark", "etl"],
) as dag:
    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    run_spark_job = BashOperator(
        task_id="run_contract_etl",
        bash_command=(
            "spark-submit "
            "--jars /opt/airflow/postgresql-42.2.5.jar "
            "/opt/airflow/scripts/contract_etl_job.py"
        ),
    )

    (start_task >> run_spark_job >> end_task)
