from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import col # type: ignore
from config.config import DB_CONFIG, ETL_CONFIG


def get_spark_session(app_name="PowerBI ETL Job"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", DB_CONFIG["driver_path"]) \
        .getOrCreate()


def extract(spark, source_table):
    print(f"[Extract] Reading from {source_table}")
    df = spark.read \
        .format("jdbc") \
        .option("url", DB_CONFIG["jdbc_url"]) \
        .option("dbtable", source_table) \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .load()
    print(f"[Extract] Retrieved {df.count()} records")
    return df


def transform_contracts(df, columns):
    print("[Transform] Contracts: Selecting required columns")
    return df.select(*[col(c) for c in columns])


def load(df, target_table, mode="overwrite"):
    print(f"[Load] Writing to {target_table} ({mode} mode)")
    df.write \
        .format("jdbc") \
        .option("url", DB_CONFIG["jdbc_url"]) \
        .option("dbtable", target_table) \
        .option("user", DB_CONFIG["user"]) \
        .option("password", DB_CONFIG["password"]) \
        .option("driver", DB_CONFIG["driver"]) \
        .mode(mode) \
        .save()
    print("[Load] Write completed")


def run_contracts_pipeline(**kwargs):
    pipeline_config = ETL_CONFIG.get("contracts_pipeline")

    source_table = kwargs.get("source_table", pipeline_config["source_table"])
    target_table = kwargs.get("target_table", pipeline_config["target_table"])
    write_mode = kwargs.get("write_mode", "overwrite")
    columns = pipeline_config["columns"]

    spark = get_spark_session()
    try:
        raw_df = extract(spark, source_table)
        transformed_df = transform_contracts(raw_df, columns)
        load(transformed_df, target_table, mode=write_mode)

        print("\n[Preview] First 5 rows:")
        transformed_df.show(5)
    finally:
        spark.stop()
        print("[Shutdown] Spark session stopped")