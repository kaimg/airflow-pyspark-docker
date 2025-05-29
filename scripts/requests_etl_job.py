from pyspark.sql.functions import col  # type: ignore
from config.config import DB_CONFIG, ETL_CONFIG
from scripts.spark_utils import create_spark_session, extract_from_jdbc, load_to_jdbc


def extract(spark, source_table):
    print(f"[Requests ETL - Extract] Reading from {source_table} via Spark Utils")
    jdbc_url = DB_CONFIG["jdbc_url"]
    properties = {
        "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"],
        "driver": DB_CONFIG["driver"],
    }
    df = extract_from_jdbc(spark, jdbc_url, source_table, properties)
    return df


def transform_contracts(df, columns):
    print("[Transform] Contracts: Selecting required columns")
    return df.select(*[col(c) for c in columns])


def load(df, target_table, mode="overwrite"):
    print(
        f"[Requests ETL - Load] Writing to {target_table} ({mode} mode) via Spark Utils"
    )
    jdbc_url = DB_CONFIG["jdbc_url"]
    properties = {
        "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"],
        "driver": DB_CONFIG["driver"],
    }
    load_to_jdbc(df, jdbc_url, target_table, mode, properties)


def run_requests_pipeline(**kwargs):
    pipeline_config = ETL_CONFIG.get("requests_pipeline")

    source_table = kwargs.get("source_table", pipeline_config["source_table"])
    target_table = kwargs.get("target_table", pipeline_config["target_table"])
    write_mode = kwargs.get("write_mode", pipeline_config["write_mode"])
    columns = pipeline_config["columns"]

    spark = create_spark_session(app_name="Requests ETL Job")
    try:
        raw_df = extract(spark, source_table)
        transformed_df = transform_contracts(raw_df, columns)
        load(transformed_df, target_table, mode=write_mode)

        print("\n[Preview] First 5 rows:")
        transformed_df.show(5)
    finally:
        spark.stop()
        print("[Shutdown] Spark session stopped")
