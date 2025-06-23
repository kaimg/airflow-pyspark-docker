from datetime import datetime
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from dags.shared_default_args import default_args
from scripts.bid_evaluation.bid_evaluation_file_upload_job import (
    run_bid_evaluation_file_upload_pipeline,
)

with DAG(
    dag_id="bid_evaluation_file_upload_pipeline_dag",
    default_args=default_args,
    description="Run PySpark ETL job for bid_evaluation_file_upload",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["pyspark", "bid_evaluation", "bid_evaluation_file_upload"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    run_spark_job = PythonOperator(
        task_id="run_bid_evaluation_file_upload_etl",
        python_callable=run_bid_evaluation_file_upload_pipeline,
        op_kwargs={
            "postgres_conn_id_destination": "postgres_conn_id_destination",
            "postgres_conn_id_source": "postgres_conn_id_source",
            "write_mode": "overwrite",
            "target_table": "bid_evaluation_file_upload",
            "sql_file_path": "dags/sql/contract/bid_evaluation_file_upload.sql",
        },
    )

    (start >> run_spark_job >> end)
