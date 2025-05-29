from pyspark.sql import SparkSession, DataFrame  # type: ignore
from pyspark.sql.functions import col  # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

from scripts.spark_utils import create_spark_session, extract_from_jdbc, load_to_jdbc


def extract(spark: SparkSession, source_table: str, jdbc_url: str, db_properties: dict):
    print(
        f"[ETL Job - Extract] Reading from {source_table} via Spark Utils using provided JDBC config"
    )
    df = extract_from_jdbc(spark, jdbc_url, source_table, db_properties)
    return df


def transform(df):
    print("[Transform] Filtering records where amount > 100...")
    transformed_df = df.filter(col("amount") > 100)
    print(f"[Transform] Transformed {transformed_df.count()} records")
    return transformed_df


def load(
    df: DataFrame,
    target_table: str,
    mode: str,
    jdbc_url: str,
    db_properties: dict,
):
    print(
        f"[ETL Job - Load] Writing to {target_table} ({mode} mode) via Spark Utils using provided JDBC config"
    )
    load_to_jdbc(df, jdbc_url, target_table, mode, db_properties)


def run_pipeline(**kwargs):
    source_table = kwargs.get("source_table", "sales")
    target_table = kwargs.get("target_table", "sales_transformed")
    write_mode = kwargs.get("write_mode", "overwrite")

    postgres_conn_id = kwargs.get("postgres_conn_id", "postgres_default")
    jdbc_driver_class = "org.postgresql.Driver"

    print(
        f"[ETL Job - Run Pipeline] Using PostgresHook with conn_id: {postgres_conn_id}"
    )
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    airflow_conn = hook.get_connection(postgres_conn_id)

    db_host = airflow_conn.host
    db_port = airflow_conn.port
    db_name = airflow_conn.schema
    db_user = airflow_conn.login
    db_password = airflow_conn.password

    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    print(f"JDBC URL: {jdbc_url}")
    db_properties = {
        "user": db_user,
        "password": db_password,
        "driver": jdbc_driver_class,
    }

    spark = create_spark_session(app_name="PySpark ETL Job (Airflow Hook)")
    try:
        raw_df = extract(spark, source_table, jdbc_url, db_properties)
        transformed_df = transform(raw_df)
        load(transformed_df, target_table, write_mode, jdbc_url, db_properties)

        print("\n[Preview] First 5 rows:")
        transformed_df.show(5)
    finally:
        spark.stop()
        print("[Shutdown] Spark session stopped")
