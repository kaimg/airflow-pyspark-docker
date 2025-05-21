from pyspark.sql import SparkSession # type: ignore
from datetime import datetime, timedelta
from config.config import DB_CONFIG, ETL_CONFIG


def get_spark_session(app_name="User Info Analytics Job"):
    print(f"[Spark Session] Initializing Spark session for '{app_name}'")
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", DB_CONFIG["driver_path"]) \
        .getOrCreate()


def extract_tables(spark, pipeline_name="user_info_pipeline"):
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


def transform_user_info_sql(spark, extracted_dfs, pipeline_name="user_info_pipeline"):
    print("[Transform] Performing user info transformation using Spark SQL...")

    pipeline_config = ETL_CONFIG[pipeline_name]
    source_tables = pipeline_config.get("source_tables", [])

    for table_info in source_tables:
        alias = table_info["alias"]
        view_name = table_info.get("view_name", alias)
        df = extracted_dfs.get(alias)
        if df is not None:
            df.createOrReplaceTempView(view_name)
        else:
            raise ValueError(f"[Error] DataFrame '{alias}' not found in extracted_dfs")

    sql_query = """
        SELECT
            u.id,
            u.first_name,
            u.last_name,
            c.id AS company_id,
            c.name AS company_name,
            r.name AS user_type
        FROM u
        LEFT JOIN c ON u.company_id = c.id
        LEFT JOIN users_user_roles ur ON u.id = ur.user_id
        LEFT JOIN roles_role r ON ur.role_id = r.id
    """

    transformed_df = spark.sql(sql_query)
    print(f"[Transform] Transformed {transformed_df.count()} records.")
    return transformed_df


def load(df, target_table, mode="overwrite"):
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


def run_user_info_pipeline(**kwargs):
    pipeline_name = "user_info_pipeline"
    pipeline_config = ETL_CONFIG.get(pipeline_name)
    target_table = kwargs.get("target_table", pipeline_config["target_table"])
    write_mode = kwargs.get("write_mode", pipeline_config["write_mode"])

    spark = get_spark_session()
    try:
        extracted_dfs = extract_tables(spark, pipeline_name)
        transformed_df = transform_user_info_sql(spark, extracted_dfs, pipeline_name)
        load(transformed_df, target_table, mode=write_mode)

        print("\n[Preview] First 5 rows of transformed data:")
        transformed_df.show(5)
    finally:
        spark.stop()
        print("[Shutdown] Spark session stopped")