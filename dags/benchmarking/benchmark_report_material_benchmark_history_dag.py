from datetime import datetime, timedelta
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from dags.shared_default_args import default_args
from scripts.benchmarking.benchmarking_report_material_job import (
    run_benchmarking_material_pipeline,
)


with DAG(
    dag_id="benchmarking_material_pipeline_dag",
    default_args=default_args,
    description="Run PySpark ETL job for user info dashboard",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["pyspark", "benchmarking", "benchmarking_material"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    run_spark_job = PythonOperator(
        task_id="run_benchmarking_material_etl",
        python_callable=run_benchmarking_material_pipeline,
        op_kwargs={
            "postgres_conn_id_destination": "postgres_conn_id_destination",
            "postgres_conn_id_source": "postgres_conn_id_source",
            "write_mode": "overwrite",
            "target_table": "currency_rates",
            "sql_file_path": "dags/sql/benchmarking/benchmark_report_material_benchmark_history.sql",
        },
    )

    (start >> run_spark_job >> end)
