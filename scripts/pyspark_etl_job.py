from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config.config import DB_CONFIG

def get_spark_session(app_name="ETL with Advanced Transformations"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "/opt/airflow/postgresql-42.2.5.jar") \
        .getOrCreate()


def extract(spark, source_table):
    jdbc_url = DB_CONFIG["jdbc_url"]
    properties = {
        "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"],
        "driver": DB_CONFIG["driver"]
    }
    print(f"[Extract] Reading from {source_table}")
    df = spark.read.jdbc(jdbc_url, source_table, properties=properties)
    print(f"[Extract] Retrieved {df.count()} records")
    return df


def transform(df):
    print("[Transform] Filtering records where amount > 100...")
    transformed_df = df.filter(col("amount") > 100)
    print(f"[Transform] Transformed {transformed_df.count()} records")
    return transformed_df


def load(df, target_table, mode="overwrite"):
    jdbc_url = DB_CONFIG["jdbc_url"]
    properties = {
        "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"],
        "driver": DB_CONFIG["driver"]
    }
    print(f"[Load] Writing to {target_table} ({mode} mode)")
    df.write.jdbc(
        jdbc_url,
        target_table,
        mode=mode,
        properties=properties
    )
    print("[Load] Write completed")


def run_pipeline(**kwargs):
    source_table = kwargs.get("source_table", "sales")
    target_table = kwargs.get("target_table", "sales_transformed")
    write_mode = kwargs.get("write_mode", "overwrite")

    spark = get_spark_session()
    try:
        raw_df = extract(spark, source_table)
        transformed_df = transform(raw_df)
        load(transformed_df, target_table, mode=write_mode)

        print("\n[Preview] First 5 rows:")
        transformed_df.show(5)
    finally:
        spark.stop()
        print("[Shutdown] Spark session stopped")