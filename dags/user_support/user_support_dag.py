from datetime import datetime, timedelta
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from scripts.user_support.user_support_job import run_user_support_pipeline

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="user_support_pipeline_dag",
    default_args=default_args,
    description="Run PySpark ETL job for user support dashboard",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["pyspark", "user_support"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    run_spark_job = PythonOperator(
        task_id="run_user_support_etl",
        python_callable=run_user_support_pipeline,
        op_kwargs={
            "postgres_conn_id_destination": "postgres_conn_id_destination",
            "postgres_conn_id_source": "postgres_conn_id_source",
            "write_mode": "overwrite",
            "target_table": "user_support_requests_request",
            "sql_file_path": "dags/sql/user_support/user_support.sql",
        },
    )

    (start >> run_spark_job >> end)
