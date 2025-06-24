from config.config import DB_CONFIG
from pathlib import Path
from scripts.logger_utils import logger
from airflow.hooks.postgres_hook import PostgresHook # type: ignore

def get_db_config(conn_id):
    """
    Returns a dictionary of PostgreSQL connection details.
    """
    conn = PostgresHook(postgres_conn_id=conn_id).get_connection(conn_id)
    return {
        'host': conn.host,
        'port': conn.port,
        'schema': conn.schema,
        'login': conn.login,
        'password': conn.password
    }

def build_jdbc_and_properties(db_config, db_config_name="source"):
    """
    Builds JDBC URL and properties for a given database config.
    Returns a tuple: (jdbc_url, db_properties)
    """
    jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['schema']}"
    db_properties = {
        "user": db_config["login"],
        "password": db_config["password"],
        "driver": DB_CONFIG["driver"],
    }
    logger.debug(f"JDBC URL ({db_config_name}): {jdbc_url}")
    return jdbc_url, db_properties

def read_sql_file(file_path: str) -> str:
    """
    Reads an SQL file and returns its content as a string.
    
    Usage in DAG:
        sql_query = read_sql_file(sql_file_path)
    """
    path = Path(file_path).resolve()

    if not path.is_file():
        logger.error(f"SQL file not found: {file_path}")
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    try:
        logger.debug(f"Reading SQL file: {file_path}")
        with open(path, "r", encoding="utf-8") as file:
            sql_query = file.read()
        logger.info(f"Successfully read SQL file: {file_path}")
        return sql_query
    except Exception as e:
        logger.exception(f"Error reading SQL file {file_path}: {e}")
        raise