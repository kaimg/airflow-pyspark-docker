from pyspark.sql import SparkSession, DataFrame  # type: ignore
from config.config import DB_CONFIG  # To access driver_path


def create_spark_session(app_name: str, additional_configs: dict = None):
    """
    Creates and returns a Spark session with common configurations.

    Args:
        app_name (str): The name for the Spark application.
        additional_configs (dict, optional): A dictionary of additional Spark configurations.
                                             Defaults to None.

    Returns:
        SparkSession: The initialized Spark session.
    """
    print(f"[Spark Utils] Initializing Spark session for '{app_name}'...")
    builder = SparkSession.builder.appName(app_name)

    # Configure JDBC driver path
    driver_path = DB_CONFIG.get("driver_path")
    if driver_path:
        builder = builder.config("spark.jars", driver_path)
        print(f"[Spark Utils] Configured spark.jars with: {driver_path}")
    else:
        # It's good to log if the driver_path is not found, as it's expected by the original functions.
        print(
            "[Spark Utils] WARNING: DB_CONFIG['driver_path'] not found. "
            "JDBC driver might not be configured if it was intended."
        )

    # Apply any additional configurations passed
    if additional_configs:
        for key, value in additional_configs.items():
            builder = builder.config(key, value)
            print(f"[Spark Utils] Applied additional config: {key}={value}")

    spark = builder.getOrCreate()
    print(f"[Spark Utils] Spark session for '{app_name}' created successfully.")
    return spark


def extract_from_jdbc(
    spark: SparkSession, jdbc_url: str, dbtable: str, properties: dict
):
    """
    Extracts data from a JDBC source.

    Args:
        spark (SparkSession): The Spark session.
        jdbc_url (str): The JDBC URL for the database connection.
        dbtable (str): The table name or a subquery to read from.
        properties (dict): Connection properties (e.g., user, password, driver).

    Returns:
        DataFrame: The DataFrame containing the extracted data.
    """
    print(
        f"[Spark Utils] Attempting to read from JDBC source. URL: {jdbc_url}, Table/Query: {dbtable}"
    )
    df = spark.read.jdbc(url=jdbc_url, table=dbtable, properties=properties)
    print(f"[Spark Utils] Successfully read data. Record count: {df.count()}")
    print(df)
    spark.catalog.listTables()
    print(spark.catalog.listTables())
    return df


def load_to_jdbc(
    df: DataFrame, jdbc_url: str, dbtable: str, mode: str, properties: dict
):
    """
    Loads a DataFrame to a JDBC target.

    Args:
        df (DataFrame): The DataFrame to load.
        jdbc_url (str): The JDBC URL for the database connection.
        dbtable (str): The target table name.
        mode (str): The write mode (e.g., "overwrite", "append").
        properties (dict): Connection properties (e.g., user, password, driver).
    """
    print(
        f"[Spark Utils] Attempting to write to JDBC target. URL: {jdbc_url}, Table: {dbtable}, Mode: {mode}"
    )
    df.write.jdbc(url=jdbc_url, table=dbtable, mode=mode, properties=properties)
    print("[Spark Utils] Successfully wrote data.")
    print(df)
