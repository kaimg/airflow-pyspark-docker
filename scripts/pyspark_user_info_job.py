from config.config import DB_CONFIG, ETL_CONFIG
from scripts.spark_utils import create_spark_session, extract_from_jdbc, load_to_jdbc
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore

def extract_tables(spark, pipeline_name="user_info_pipeline", jdbc_url=None, db_properties=None):
    print(
        f"[User Info Job - Extract] Reading tables for pipeline '{pipeline_name}' via Spark Utils..."
    )
    pipeline_config = ETL_CONFIG[pipeline_name]
    source_tables = pipeline_config.get("source_tables", [])
    extracted_dfs = {}

    base_properties = db_properties

    for table_info in source_tables:
        alias = table_info["alias"]
        schema = table_info.get(
            "schema", "public"
        )  # Default to public if not specified
        table_name = table_info["name"]
        dbtable = f"{schema}.{table_name}"

        print(f"[User Info Job - Extract] Reading from {dbtable} as '{alias}'")
        df = extract_from_jdbc(spark, jdbc_url, dbtable, base_properties)
        extracted_dfs[alias] = df

    return extracted_dfs


def transform_user_info_sql(spark, extracted_dfs, pipeline_name="user_info_pipeline", jdbc_url=None, db_properties=None, sql_file_path=None):
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

    with open(sql_file_path, 'r') as file:
        sql_query = file.read()

    transformed_df = spark.sql(sql_query)
    print(f"[Transform] Transformed {transformed_df.count()} records.")
    return transformed_df


def load(df, target_table, mode="overwrite", jdbc_url=None, db_properties=None):
    print(
        f"[User Info Job - Load] Writing to {target_table} ({mode} mode) via Spark Utils"
    )
    properties = db_properties
    load_to_jdbc(df, jdbc_url, f"public.{target_table}", mode, properties)


def run_user_info_pipeline(**kwargs):
    pipeline_name = "user_info_pipeline"
    pipeline_config = ETL_CONFIG.get(pipeline_name)
    target_table = kwargs.get("target_table", pipeline_config["target_table"])
    write_mode = kwargs.get("write_mode", pipeline_config["write_mode"])
    postgres_conn_id = kwargs.get("postgres_conn_id", "postgres_test")
    sql_file_path = kwargs.get("sql_file_path", "dags/sql/contract_user_info.sql")

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
        "driver": DB_CONFIG["driver"],
    }

    spark = create_spark_session(app_name="User Info Analytics Job")
    try:
        extracted_dfs = extract_tables(spark, pipeline_name, jdbc_url, db_properties)
        transformed_df = transform_user_info_sql(spark, extracted_dfs, pipeline_name, jdbc_url, db_properties, sql_file_path)
        load(transformed_df, target_table, mode=write_mode, jdbc_url=jdbc_url, db_properties=db_properties)

        print("\n[Preview] First 5 rows of transformed data:")
        transformed_df.show(5)
    finally:
        spark.stop()
        print("[Shutdown] Spark session stopped")
