from dotenv import load_dotenv  # type: ignore
import os
import yaml

load_dotenv()

DB_CONFIG = {
    "jdbc_url": os.getenv("DB_JDBC_URL"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": os.getenv("DB_DRIVER"),
    "driver_path": os.getenv("DB_DRIVER_PATH")
}

API_KEY_CURRENCY_RATE = os.getenv("API_KEY_CURRENCY_RATE")

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "etl_config.yaml")
with open(CONFIG_PATH, "r") as f:
    ETL_CONFIG = yaml.safe_load(f)