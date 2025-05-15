from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.functions import col  # type: ignore
from config.config import DB_CONFIG

spark = SparkSession.builder \
    .appName("ETL - Contract Data") \
    .config("spark.jars", DB_CONFIG["driver_path"]) \
    .getOrCreate()

jdbc_url = DB_CONFIG["jdbc_url"]
properties = {
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "driver": DB_CONFIG["driver"]
}

tables = {
    "contract_service_contracts": {
        "target_table": "user_support_contract_info",
        "columns": ["id", "description", "cam_id", "procurement_contract_owner", "valid_to"]
    },
    "contract_service_pricebooks": {
        "target_table": "user_support_pricebook_product",
        "columns": ["id", "supplier_description", "price_valid_to"]
    },
    "contract_service_requests_technicalrequest": {
        "target_table": "user_support_requests_technical_request",
        "columns": ["id", "type", "status", "request_id"]
    },
    "contract_service_requests": {
        "target_table": "user_support_requests_request",
        "columns": ["id", "request_to", "pricebook_id", "created_at", "status", "receiver_id"]
    }
}


def extract_transform_load(source_table, target_table, columns):
    try:
        print(f"Processing table: {source_table} -> {target_table}")
        df = spark.read.jdbc(jdbc_url, source_table, properties=properties)

        selected_df = df.select(*columns)

        selected_df.write \
            .jdbc(url=jdbc_url,
                  table=target_table,
                  mode="overwrite",
                  properties=properties)
        print(f"Successfully loaded data into {target_table}")
    except Exception as e:
        print(f"Error processing {source_table}: {str(e)}")


if __name__ == "__main__":
    for source, meta in tables.items():
        extract_transform_load(source, meta["target_table"], meta["columns"])

    spark.stop()