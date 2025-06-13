import requests
from airflow.sdk import Variable
from config.config import DB_CONFIG
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from pyspark.sql.types import StructType, StructField, StringType, DoubleType  # type: ignore
from scripts.spark_utils import create_spark_session, extract_from_jdbc, load_to_jdbc

ETL_CONFIG = {
    "currency_rates_pipeline": {
        "target_table": "currency_rates",
        "write_mode": "overwrite"
    }
}

def get_api_response(sparkSession):
    API_KEY_CURRENCY_RATE = Variable.get("API_KEY_CURRENCY_RATE")
    API_URL = "http://api.exchangeratesapi.io/v1/"
    TARGET_CURRENCIES = ["USD", "AUD", "CAD", "PLN", "MXN", "AZN", "TRY"]
    
    symbols = ",".join(TARGET_CURRENCIES)
    params = {"access_key": API_KEY_CURRENCY_RATE, "base": "EUR", "symbols": symbols}
    response = requests.get(f"{API_URL}/latest", params=params)

    if response.status_code != 200:
        raise Exception("API request failed")

    data = response.json()
    if not data.get("success"):
        raise Exception(data.get("message", "Unknown error"))

    base = data["base"]
    date = data["date"]
    rates = data["rates"]

    rows = [
        {"target_currency": currency, "rate": rate, "base": base, "date": date}
        for currency, rate in rates.items()
    ]

    # Define schema
    schema = StructType(
        [
            StructField("target_currency", StringType(), True),
            StructField("rate", DoubleType(), True),
            StructField("base", StringType(), True),
            StructField("date", StringType(), True),
        ]
    )

    df = sparkSession.createDataFrame(rows, schema=schema)
    df.show(5)
    return df
def extract_tables(spark, pipeline_name="currency_rates_pipeline", jdbc_url=None, db_properties=None):
    
    print(
        f"[User Info Job - Extract] Reading tables for pipeline '{pipeline_name}' via Spark Utils..."
    )

    extracted_dfs = get_api_response(sparkSession=spark)
    return extracted_dfs


def transform_user_info_sql(spark, extracted_dfs, pipeline_name="currency_rates_pipeline", jdbc_url=None, db_properties=None, sql_file_path=None):
    print("[Transform] Performing user info transformation using Spark SQL...")
    return extracted_dfs


def load(df, target_table, mode="overwrite", jdbc_url=None, db_properties=None):
    print(
        f"[User Info Job - Load] Writing to {target_table} ({mode} mode) via Spark Utils"
    )
    properties = db_properties
    load_to_jdbc(df, jdbc_url, f"{target_table}", mode, properties)


def run_currency_rates_pipeline(**kwargs):
    pipeline_name = "currency_rates_pipeline"
    pipeline_config = ETL_CONFIG.get(pipeline_name)
    target_table = kwargs.get("target_table", pipeline_config["target_table"])
    #write_mode = kwargs.get("write_mode", pipeline_config["write_mode"])
    postgres_conn_id_destination = kwargs.get("postgres_conn_id_destination", "postgres_conn_id_destination")

    hook_destination = PostgresHook(postgres_conn_id=postgres_conn_id_destination)
    airflow_conn_destination = hook_destination.get_connection(postgres_conn_id_destination) 
    db_host_destination = airflow_conn_destination.host
    db_port_destination = airflow_conn_destination.port
    db_name_destination = airflow_conn_destination.schema
    db_user_destination = airflow_conn_destination.login
    db_password_destination = airflow_conn_destination.password

    jdbc_url_destination = f"jdbc:postgresql://{db_host_destination}:{db_port_destination}/{db_name_destination}"
    print(f"JDBC URL Destination: {jdbc_url_destination}")

    db_properties_destination = {
        "user": db_user_destination,
        "password": db_password_destination,
        "driver": DB_CONFIG["driver"],
    }

    spark = create_spark_session(app_name="Currency Rates Analytics Job")
    try:
        extracted_dfs = extract_tables(spark, pipeline_name, jdbc_url=None, db_properties=None)
        transformed_df = transform_user_info_sql(spark, extracted_dfs, pipeline_name, jdbc_url=None, db_properties=None, sql_file_path=None)
        load(transformed_df, target_table, jdbc_url=jdbc_url_destination, db_properties=db_properties_destination)

        print("\n[Preview] First 5 rows of transformed data:")
        transformed_df.show(5)
    finally:
        spark.stop()
        print("[Shutdown] Spark session stopped")
