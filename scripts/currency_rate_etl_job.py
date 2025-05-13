import requests
import polars as pl
from config.config import API_KEY_CURRENCY_RATE

API_URL = f"http://api.exchangeratesapi.io/v1/latest?access_key={API_KEY_CURRENCY_RATE}"
def get_currency_rate(currency_code: str) -> float:
    response = requests.get(f"{API_URL}&{currency_code}&symbols=USD,AUD,CAD,PLN,MXN")
    data = response.json()
    print(data)
    if not data.get("success"):
        raise Exception("API request failed or invalid response")

    base = data["base"]
    date = data["date"]
    rates = data["rates"]

    rows = [
        {"Target Currency": currency, "Rate": rate, "Base": base, "Date": date}
        for currency, rate in rates.items()
    ]

    df = pl.DataFrame(rows)

    return df

df = get_currency_rate("")
df.write_csv("./data/currency_rates.csv")

