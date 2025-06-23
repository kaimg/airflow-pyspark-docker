from config.config import DB_CONFIG
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
    print(f"JDBC URL ({db_config_name}): {jdbc_url}")
    return jdbc_url, db_properties