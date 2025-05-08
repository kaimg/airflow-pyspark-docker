from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Set up Spark session
spark = SparkSession.builder \
    .appName("ETL with Advanced Transformations") \
    .config("spark.jars", "/opt/airflow/postgresql-42.2.5.jar") \
    .getOrCreate()

# JDBC URLs for Neon.tech
jdbc_url = "jdbc:postgresql://ep-flat-frost-a2at8amr-pooler.eu-central-1.aws.neon.tech/pipeline-test?sslmode=require"

# Properties
properties = {
    "user": "pipeline-test_owner",
    "password": "npg_Bwhe5v1KHlng",
    "driver": "org.postgresql.Driver"
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
