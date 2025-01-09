from datetime import datetime
from typing import Dict, Any
import requests
import pandas as pd


def fetch_transform_stock(
    stock_base_url: str,
    stock_api_key: str,
    function: str,
    symbol: str,
    outputsize: str,
    days_to_keep: int = 7,
) -> Dict[str, Any]:
    """
    Fetch stock data from Alpha Vantage and transform into a list of dictionaries.
    """
    params = {
        "function": function,
        "symbol": symbol,
        "outputsize": outputsize,
        "apikey": stock_api_key,
    }

    try:
        resp = requests.get(stock_base_url, params=params, timeout=60)
        resp.raise_for_status()
        stock_json = resp.json()
    except requests.RequestException as e:
        print(f"Error fetching stock data: {e}")
        return {"records": []}

    if "Time Series (Daily)" not in stock_json:
        print("Malformed stock data: 'Time Series (Daily)' missing.")
        return {"records": []}

    stock_symbol = stock_json["Meta Data"].get("2. Symbol", "UNKNOWN")
    time_series = stock_json["Time Series (Daily)"]

    df = pd.DataFrame.from_dict(time_series, orient="index")
    df.columns = ["open", "high", "low", "close", "volume"]

    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["stock_symbol"] = stock_symbol
    df.reset_index(inplace=True)
    df.rename(columns={"index": "date"}, inplace=True)

    df = df.head(days_to_keep)

    df["partition_hour"] = datetime.now().strftime("%Y%m%d%H")

    return {"records": df.to_dict(orient="records")}
