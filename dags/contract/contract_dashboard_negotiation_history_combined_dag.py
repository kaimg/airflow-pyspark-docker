from datetime import datetime, timedelta
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from dags.shared_default_args import default_args
from scripts.contract.contract_dashboard_negotiation_history_combined_job import (
    run_contract_dashboard_negotiation_history_combined_pipeline,
)

with DAG(
    dag_id="contract_dashboard_negotiation_history_combined_pipeline_dag",
    default_args=default_args,
    description="Run PySpark ETL job for contract_dashboard_negotiation_history_combined",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=[
        "pyspark", 
        "contract", 
        "contract_dashboard_negotiation_history_combined"
    ],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    run_spark_job = PythonOperator(
        task_id="run_contract_dashboard_negotiation_history_combined_etl",
        python_callable=run_contract_dashboard_negotiation_history_combined_pipeline,
        op_kwargs={
            "postgres_conn_id_destination": "postgres_conn_id_destination",
            "postgres_conn_id_source": "postgres_conn_id_source",
            "write_mode": "overwrite",
            "target_table": "contract_dashboard_negotiation_history_combined",
            "sql_file_path": "dags/sql/contract/contract_dashboard_negotiation_history_combined.sql",
        },
    )

    (start >> run_spark_job >> end)
