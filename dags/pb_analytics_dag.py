from datetime import datetime, timedelta
import os
from config.config import ETL_CONFIG
from airflow import DAG  # type: ignore
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sql')


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pricebook_analytics_pure_sql_etl",
    default_args=default_args,
    description="ETL pipeline for Pricebook Analytics using PostgreSQL for transformations, orchestrated by SQL files.",
    start_date=datetime(2025, 1, 1),
    catchup=False, # Set to False to prevent backfilling from start_date to now
    tags=["postgresql", "etl", "analytics", "sql-files"],
    template_searchpath=[SQL_DIR], # Crucial: Airflow will look for .sql files here
) as dag:

    start = EmptyOperator(task_id="start_pipeline")

    # Task 1: Prepare the staging table (Create if not exists, then Truncate)
    create_and_truncate_staging = SQLExecuteQueryOperator(
        task_id='create_and_truncate_staging_table',
        conn_id='your_postgres_conn_id', # !!! IMPORTANT: Configure this in Airflow UI !!!
        sql='create_staging_table.sql', # Refers to the file in template_searchpath
    )

    # Task 2: Perform the main complex transformation
    transform_data_to_staging = SQLExecuteQueryOperator(
        task_id='transform_data_to_staging',
        conn_id='your_postgres_conn_id',
        sql='pricebook_analytics.sql', 
    )

    # Task 3: Load the transformed data from staging to the final target table
    load_final_output = SQLExecuteQueryOperator(
        task_id='load_transformed_data_to_final',
        conn_id='your_postgres_conn_id',
        sql='load_final_table.sql', # Refers to the file in template_searchpath
        parameters={
            'target_table': ETL_CONFIG['pricebook_product_pipeline']['target_table'],
        }
    )

    end = EmptyOperator(task_id="end_pipeline")

    # --- Define Task Dependencies ---
    start >> create_and_truncate_staging >> transform_data_to_staging >> load_final_output >> end