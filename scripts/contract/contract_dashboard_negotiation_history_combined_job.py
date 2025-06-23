from scripts.spark_utils import create_spark_session, extract_from_jdbc, load_to_jdbc
from scripts.pg_db_utils import get_db_config, build_jdbc_and_properties

ETL_CONFIG = {
    "contract_dashboard_negotiation_history_combined_pipeline": {
        "source_tables": [
            {
                "name": "contracts_contract",
                "schema": "public",
                "alias": "contracts_contract",
            }
        ],
        "target_table": "contract_dashboard_negotiation_history_combined",
        "write_mode": "overwrite",
    }
}


def extract_tables(
    spark,
    pipeline_name="contract_dashboard_negotiation_history_combined_pipeline",
    jdbc_url=None,
    db_properties=None,
):
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


def transform_contract_dashboard_negotiation_history_combined_sql(
    spark,
    extracted_dfs,
    pipeline_name="contract_dashboard_negotiation_history_combined_pipeline",
    jdbc_url=None,
    db_properties=None,
    sql_file_path=None,
):
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

    with open(sql_file_path, "r") as file:
        sql_query = file.read()

    transformed_df = spark.sql(sql_query)
    print(f"[Transform] Transformed {transformed_df.count()} records.")
    print(transformed_df)
    return transformed_df


def load(df, target_table, mode="overwrite", jdbc_url=None, db_properties=None):
    print(
        f"[User Info Job - Load] Writing to {target_table} ({mode} mode) via Spark Utils"
    )
    properties = db_properties
    load_to_jdbc(df, jdbc_url, f"{target_table}", mode, properties)


def run_contract_dashboard_negotiation_history_combined_pipeline(**kwargs):
    pipeline_name = "contract_dashboard_negotiation_history_combined_pipeline"
    pipeline_config = ETL_CONFIG.get(pipeline_name)
    target_table = kwargs.get("target_table", pipeline_config["target_table"])
    # write_mode = kwargs.get("write_mode", pipeline_config["write_mode"])
    postgres_conn_id_source = kwargs.get(
        "postgres_conn_id_source", "postgres_conn_id_source"
    )
    postgres_conn_id_destination = kwargs.get(
        "postgres_conn_id_destination", "postgres_conn_id_destination"
    )
    sql_file_path = kwargs.get(
        "sql_file_path", "dags/sql/contract/contract_dashboard_negotiation_history_combined.sql"
    )
    db_source = get_db_config(postgres_conn_id_source)
    db_destination = get_db_config(postgres_conn_id_destination)

    jdbc_url_source, db_properties_source = build_jdbc_and_properties(db_source, "source")
    jdbc_url_destination, db_properties_destination = build_jdbc_and_properties(db_destination, "destination")

    spark = create_spark_session(app_name="Benchmarking Material Analytics Job")
    try:
        extracted_dfs = extract_tables(spark, pipeline_name, jdbc_url_source, db_properties_source)
        transformed_df = transform_contract_dashboard_negotiation_history_combined_sql(
            spark, extracted_dfs, pipeline_name, jdbc_url_source, db_properties_source, sql_file_path
        )
        load(
            transformed_df,
            target_table,
            jdbc_url=jdbc_url_destination,
            db_properties=db_properties_destination,
        )

        print("\n[Preview] First 5 rows of transformed data:")
        transformed_df.show(5)
    finally:
        spark.stop()
        print("[Shutdown] Spark session stopped")
