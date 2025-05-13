import requests
import polars as pl
from config.config import API_KEY_CURRENCY_RATE
from pathlib import Path

API_URL = f"http://api.exchangeratesapi.io/v1/"
TARGET_CURRENCIES = ["USD","AUD","CAD","PLN","MXN"]

OUTPUT_DIR = Path(__file__).parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "currency_rates.csv"
OUTPUT_FILE_CURRENCIES = OUTPUT_DIR / "currencies_data.json"

def get_currency_rate(currency_code: str="EUR") -> float:
    symbols = ",".join(TARGET_CURRENCIES)
    params = {
        "access_key": API_KEY_CURRENCY_RATE,
        "base": currency_code,
        "symbols": symbols
    }
    response = requests.get(f"{API_URL}/latest", params=params)

    if response.status_code != 200:
        raise Exception("API request failed")
    
    data = response.json()
    if not data.get("success"):
        raise Exception(data.get("message", "Unknown error"))
    
    #print(data)
    
    base = data["base"]
    date = data["date"]
    rates = data["rates"]

    rows = [
        {"Target Currency": currency, "Rate": rate, "Base": base, "Date": date}
        for currency, rate in rates.items()
    ]

    df = pl.DataFrame(rows)
    df.write_csv(OUTPUT_FILE)
    return df

def get_currencies_names():
    params = {
        "access_key": API_KEY_CURRENCY_RATE,
    }
    response = requests.get(f"{API_URL}/symbols", params=params)
    data = response.json()

    data = data["symbols"]
    
    df = pl.DataFrame(data)
    df.write_json(OUTPUT_FILE_CURRENCIES)

df = get_currency_rate("")

