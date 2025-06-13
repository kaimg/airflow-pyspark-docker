from config.config import DB_CONFIG, ETL_CONFIG  # Ensure config is correctly imported
from datetime import datetime, timedelta
from scripts.spark_utils import create_spark_session, extract_from_jdbc, load_to_jdbc


def extract_tables(spark, pipeline_name="pricebook_product_pipeline"):
    print(
        f"[Pricebook Analytics - Extract] Reading tables for pipeline '{pipeline_name}' via Spark Utils..."
    )
    pipeline_config = ETL_CONFIG[pipeline_name]
    source_tables = pipeline_config.get("source_tables", [])
    extracted_dfs = {}

    jdbc_url = DB_CONFIG["jdbc_url"]
    base_properties = {
        "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"],
        "driver": DB_CONFIG["driver"],
    }

    for table_info in source_tables:
        alias = table_info["alias"]
        schema = table_info.get("schema", "public")
        table_name = table_info["name"]
        dbtable = f"{schema}.{table_name}"

        print(f"[Pricebook Analytics - Extract] Reading from {dbtable} as '{alias}'")
        df = extract_from_jdbc(spark, jdbc_url, dbtable, base_properties)
        extracted_dfs[alias] = df

    return extracted_dfs


def transform_complex_analytics_spark_sql(
    spark, extracted_dfs, last_updated_value, pipeline_name="pricebook_product_pipeline"
):
    print("[Transform] Performing complex analytics transformation using Spark SQL...")

    # Get pipeline config
    pipeline_config = ETL_CONFIG[pipeline_name]
    source_tables = pipeline_config.get("source_tables", [])

    # Register all DataFrames dynamically using either 'view_name' or 'alias'
    for table_info in source_tables:
        alias = table_info["alias"]
        view_name = table_info.get("view_name", alias)  # fallback to alias
        df = extracted_dfs.get(alias)
        if df is not None:
            df.createOrReplaceTempView(view_name)
        else:
            raise ValueError(f"[Error] DataFrame '{alias}' not found in extracted_dfs")

    # Define the complex SQL query
    # Note: Spark SQL's date/timestamp literal format might differ slightly,
    # or you might use `TO_TIMESTAMP()` if 'updated_at' is a string.
    # We're using f-string for direct injection of last_updated_value.
    sql_file_path = "dags/sql/pricebook_analytics.sql"
    with open(sql_file_path, "r") as file:
        sql_query = file.read()

    transformed_df = spark.sql(sql_query)
    print(f"[Transform] Transformed {transformed_df.count()} records.")
    return transformed_df


def load(df, target_table, mode="overwrite"):
    print(
        f"[Pricebook Analytics - Load] Writing to {target_table} ({mode} mode) via Spark Utils"
    )
    jdbc_url = DB_CONFIG["jdbc_url"]
    properties = {
        "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"],
        "driver": DB_CONFIG["driver"],
    }
    load_to_jdbc(df, jdbc_url, f"public.{target_table}", mode, properties)


def run_pricebook_analytics_pipeline(**kwargs):
    # Dynamic 'last_updated_value' using Airflow's execution_date
    # For daily runs, this will be the previous day's run.
    # Adjust as per your incremental logic (e.g., using max(updated_at) from target table)
    # The format 'YYYY-MM-DD HH:MM:SS' is standard for SQL WHERE clauses.
    last_updated_value = kwargs.get(
        "last_updated_value",
        (datetime.now() - timedelta(days=1)).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),  # Default to yesterday
    )

    pipeline_config = ETL_CONFIG.get("pricebook_product_pipeline")
    # Define your target table for the analytics output
    # This might be different from your existing contract_dashboard_contract_info
    target_table = kwargs.get("target_table", pipeline_config["target_table"])
    write_mode = kwargs.get(
        "write_mode", pipeline_config["write_mode"]
    )  # Or 'append' for incremental

    spark = create_spark_session(app_name="Pricebook Analytics ETL Job")
    try:
        # Extract all necessary tables
        extracted_dfs = extract_tables(spark)

        # Perform the complex transformation using Spark SQL
        transformed_df = transform_complex_analytics_spark_sql(
            spark, extracted_dfs, last_updated_value
        )

        # Load the result into your target table in PostgreSQL
        load(transformed_df, target_table, mode=write_mode)

        print("\n[Preview] First 5 rows of transformed data:")
        transformed_df.show(5)
    finally:
        spark.stop()
        print("[Shutdown] Spark session stopped")
