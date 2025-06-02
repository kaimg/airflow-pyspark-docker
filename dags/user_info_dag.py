from datetime import datetime, timedelta
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from scripts.pyspark_user_info_job import run_user_info_pipeline

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="user_info_pipeline_dag",
    default_args=default_args,
    description="Run PySpark ETL job for user info dashboard",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["pyspark", "user_info"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    run_spark_job = PythonOperator(
        task_id="run_user_info_etl",
        python_callable=run_user_info_pipeline,
        op_kwargs={
            "postgres_conn_id_destination": "postgres_test",
            "postgres_conn_id_source": "postgres_default",
            "sql_file_path": "dags/sql/contract_user_info.sql",
        },
    )

    (start >> run_spark_job >> end)
