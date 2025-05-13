from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config.config import DB_CONFIG

# Set up Spark session
spark = SparkSession.builder \
    .appName("ETL with Advanced Transformations") \
    .config("spark.jars", "/opt/airflow/postgresql-42.2.5.jar") \
    .getOrCreate()

# JDBC URLs for Neon.tech
jdbc_url = DB_CONFIG["jdbc_url"]

# Properties
properties = {
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "driver": DB_CONFIG["driver"]
}

# Read from source table (e.g., "sales")
sales_df = spark.read.jdbc(jdbc_url, "sales", properties=properties)

# Simple transformation
transformed_df = sales_df.filter(col("amount") > 100)

# Write to destination table
transformed_df.write.jdbc(jdbc_url, "sales_transformed", mode="overwrite", properties=properties)

# Optional preview
transformed_df.show()

# Shutdown
spark.stop()
