import requests
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.types import StructType, StructField, StringType, DoubleType # type: ignore
from config.config import API_KEY_CURRENCY_RATE, DB_CONFIG

API_URL = f"http://api.exchangeratesapi.io/v1/"
TARGET_CURRENCIES = ["USD", "AUD", "CAD", "PLN", "MXN", "AZN", "TRY"]


def get_spark_session(app_name="Currency Rate ETL Job"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", DB_CONFIG["driver_path"]) \
        .getOrCreate()
    return spark


def get_currency_rate(spark: SparkSession):
    symbols = ",".join(TARGET_CURRENCIES)
    params = {
        "access_key": API_KEY_CURRENCY_RATE,
        "base": "EUR",
        "symbols": symbols
    }
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
    schema = StructType([
        StructField("target_currency", StringType(), True),
        StructField("rate", DoubleType(), True),
        StructField("base", StringType(), True),
        StructField("date", StringType(), True)
    ])

    df = spark.createDataFrame(rows, schema=schema)
    return df


def load_currency_rates_to_db(df, target_table="currency_rates", mode="overwrite"):
    jdbc_url = DB_CONFIG["jdbc_url"]
    properties = {
        "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"],
        "driver": DB_CONFIG["driver"]
    }

    print(f"[Load] Writing {df.count()} records to {target_table}")
    df.write.jdbc(
        url=jdbc_url,
        table=target_table,
        mode=mode,
        properties=properties
    )
    print("[Load] Write completed")


def run_currency_etl_job():
    spark = get_spark_session()
    try:
        df = get_currency_rate(spark)
        load_currency_rates_to_db(df, mode="overwrite")
        df.show(5)
    finally:
        spark.stop()
        print("[Shutdown] Spark session stopped")


if __name__ == "__main__":
    run_currency_etl_job()