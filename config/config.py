from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    "jdbc_url": os.getenv("DB_JDBC_URL"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": os.getenv("DB_DRIVER")
}

API_KEY_CURRENCY_RATE = os.getenv("API_KEY_CURRENCY_RATE")