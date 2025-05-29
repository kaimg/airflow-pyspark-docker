from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.generic_transfer import GenericTransfer

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- ETL Configuration ---
SOURCE_TABLE = "sales"
TARGET_TABLE = "sales_transformed"
# Transformation: select all columns from source_table where amount > 100
SQL_QUERY_FOR_TRANSFER = f"SELECT * FROM {SOURCE_TABLE} WHERE amount > 100"
POSTGRES_CONN_ID = "postgres_default" # Airflow Connection ID for PostgreSQL
WRITE_MODE = "overwrite" # Supported modes: "overwrite", "append" (append is default if no preoperator)

# --- DAG Definition ---
with DAG(
    dag_id="etl_sales_data_generic_transfer",
    default_args=default_args,
    description="ETL pipeline for sales data using GenericTransfer. Extracts, transforms (filters), and loads.",
    start_date=datetime(2025, 1, 1), # Adjust start_date as needed
    catchup=False,
    tags=["etl", "generic_transfer", "sales"],
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    # --- Pre-operator command for handling write_mode ---
    # This list will hold SQL commands to run before the main transfer.
    pre_operator_sql_commands = []

    if WRITE_MODE == "overwrite":
        # TRUNCATE TABLE is generally faster than DELETE FROM for clearing all rows.
        # Ensure the user executing this has TRUNCATE permissions.
        # This command assumes the target table already exists.
        pre_operator_sql_commands.append(f"TRUNCATE TABLE {TARGET_TABLE};")
        print(f"Configured TRUNCATE command for {TARGET_TABLE} due to overwrite mode.")

    # --- GenericTransfer Task Definition ---
    transfer_and_transform_data = GenericTransfer(
        task_id="transfer_and_transform_sales_data",
        sql=SQL_QUERY_FOR_TRANSFER,
        destination_table=TARGET_TABLE,
        source_conn_id=POSTGRES_CONN_ID,      # Connection ID for the source database
        destination_conn_id=POSTGRES_CONN_ID, # Connection ID for the destination database
        preoperator=pre_operator_sql_commands if pre_operator_sql_commands else None,
        page_size=1000,
        # If your GenericTransfer supports `execution_timeout` or other params, add them here.
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # --- Task Dependencies ---
    start_pipeline >> transfer_and_transform_data >> end_pipeline   