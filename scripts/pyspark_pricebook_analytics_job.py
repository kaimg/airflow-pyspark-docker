from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import col, lit # type: ignore
from config.config import DB_CONFIG, ETL_CONFIG # Ensure config is correctly imported
from datetime import datetime, timedelta

def get_spark_session(app_name="Pricebook Analytics ETL Job"):
    print(f"[Spark Session] Initializing Spark session for '{app_name}'")
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", DB_CONFIG["driver_path"]) \
        .getOrCreate()

def extract_tables(spark, pipeline_name="pricebook_product_pipeline"):
    print(f"[Extract] Reading tables for pipeline '{pipeline_name}'...")
    pipeline_config = ETL_CONFIG[pipeline_name]
    source_tables = pipeline_config.get("source_tables", [])
    extracted_dfs = {}

    for table_info in source_tables:
        alias = table_info["alias"]
        schema = table_info.get("schema", "public")  # Default to public if not specified
        table_name = table_info["name"]
        dbtable = f"{schema}.{table_name}"

        print(f"[Extract] Reading from {dbtable} as '{alias}'")
        df = spark.read \
            .format("jdbc") \
            .option("url", DB_CONFIG["jdbc_url"]) \
            .option("dbtable", dbtable) \
            .option("user", DB_CONFIG["user"]) \
            .option("password", DB_CONFIG["password"]) \
            .option("driver", DB_CONFIG["driver"]) \
            .load()

        record_count = df.count()
        print(f"[Extract] Retrieved {record_count} records from {dbtable}")
        extracted_dfs[alias] = df

    return extracted_dfs

def transform_complex_analytics_spark_sql(spark, extracted_dfs, last_updated_value, pipeline_name="pricebook_product_pipeline"):
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
    sql_query = f"""
    SELECT
        ppp.id,
        ppp.pricebook_id,
        ppp.status,
        ppp.is_new,
        ppp.cam_approved,
        ppp.disable_reason,
        ppp.updated_at,
        ppp.product_id,
        ppp.is_new_for_contract_owner,

        MAX(CASE WHEN prf.field_name = 'supplier_uom' THEN pbv.field_value END) AS supplier_uom,
        MAX(CASE WHEN prf.field_name = 'supplier_description' THEN pbv.field_value END) AS supplier_description,
        MAX(CASE WHEN prf.field_name = 'supplier_part_no' THEN pbv.field_value END) AS supplier_part_no,
        MAX(CASE WHEN prf.field_name = 'unit_price' THEN pbv.field_value END) AS unit_price,
        MAX(CASE WHEN prf.field_name = 'price_valid_from' THEN pbv.field_value END) AS price_valid_from,
        MAX(CASE WHEN prf.field_name = 'price_valid_to' THEN pbv.field_value END) AS price_valid_to,
        MAX(CASE WHEN prf.field_name = 'sla_lead_time' THEN pbv.field_value END) AS sla_lead_time,
        MAX(CASE WHEN prf.field_name = 'incoterm_key' THEN pbv.field_value END) AS incoterm_key,
        MAX(CASE WHEN prf.field_name = 'minimum_order_quantity' THEN pbv.field_value END) AS minimum_order_quantity,
        MAX(CASE WHEN prf.field_name = 'maximum_order_quantity' THEN pbv.field_value END) AS maximum_order_quantity,
        MAX(CASE WHEN prf.field_name = 'customer_short_description' THEN pbv.field_value END) AS customer_short_description,
        MAX(CASE WHEN prf.field_name = 'sap_id' THEN pbv.field_value END) AS customer_unique_id,
        MAX(CASE WHEN prf.field_name = 'manufacturer_name' THEN pbv.field_value END) AS manufacturer_name,
        MAX(CASE WHEN prf.field_name = 'manufacturer_part_no' THEN pbv.field_value END) AS manufacturer_part_no,
        MAX(CASE WHEN prf.field_name = 'manufacturer_description' THEN pbv.field_value END) AS manufacturer_description,

        pb.contract_id AS contract_id,
        pb.description AS pricebook_description,
        pb.currency AS pricebook_currency,
        pb.customer_reference_number AS pricebook_customer_reference_number,
        NULL AS last_download_date -- Placeholder, or calculate if needed
    FROM
        ppp
    JOIN
        pbv
        ON ppp.product_id = pbv.pb_product_id
    JOIN
        pb_required_field prf
        ON prf.id = pbv.pb_required_field_id
    INNER JOIN pb
        ON ppp.pricebook_id = pb.id
    WHERE pbv.updated_at > '{last_updated_value}'
    GROUP BY
        ppp.id,
        ppp.pricebook_id,
        ppp.status,
        ppp.is_new,
        ppp.cam_approved,
        ppp.disable_reason,
        ppp.updated_at,
        ppp.product_id,
        ppp.is_new_for_contract_owner,
        pb.contract_id,
        pb.description,
        pb.currency,
        pb.customer_reference_number
    """
    transformed_df = spark.sql(sql_query)
    print(f"[Transform] Transformed {transformed_df.count()} records.")
    return transformed_df

def load(df, target_table, mode="overwrite"):
    """
    Loads a Spark DataFrame into a PostgreSQL table.
    """
    print(f"[Load] Writing to {target_table} ({mode} mode)")
    df.write \
        .format("jdbc") \
        .option("url", DB_CONFIG["jdbc_url"]) \
        .option("dbtable", f"public.{target_table}") \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .mode(mode) \
        .save()
    print("[Load] Write completed")

def run_pricebook_analytics_pipeline(**kwargs):
    # Dynamic 'last_updated_value' using Airflow's execution_date
    # For daily runs, this will be the previous day's run.
    # Adjust as per your incremental logic (e.g., using max(updated_at) from target table)
    # The format 'YYYY-MM-DD HH:MM:SS' is standard for SQL WHERE clauses.
    last_updated_value = kwargs.get(
        "last_updated_value",
        (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S") # Default to yesterday
    )

    pipeline_config = ETL_CONFIG.get("pricebook_product_pipeline")
    # Define your target table for the analytics output
    # This might be different from your existing contract_dashboard_contract_info
    target_table = kwargs.get("target_table", pipeline_config["target_table"])
    write_mode = kwargs.get("write_mode", pipeline_config["write_mode"]) # Or 'append' for incremental

    spark = get_spark_session()
    try:
        # Extract all necessary tables
        extracted_dfs = extract_tables(spark)
        
        # Perform the complex transformation using Spark SQL
        transformed_df = transform_complex_analytics_spark_sql(spark, extracted_dfs, last_updated_value)
        
        # Load the result into your target table in PostgreSQL
        load(transformed_df, target_table, mode=write_mode)

        print("\n[Preview] First 5 rows of transformed data:")
        transformed_df.show(5)
    finally:
        spark.stop()
        print("[Shutdown] Spark session stopped")