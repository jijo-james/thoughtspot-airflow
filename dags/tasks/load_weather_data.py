from typing import Dict, Any
import sqlite3
import pandas as pd


def load_weather_data(db_name: str, weather_records: Dict[str, Any]) -> None:
    """
    Load weather data into SQLite (table: weather_data).
    """
    records = weather_records.get("records", [])
    if not records:
        print("No weather records to load.")
        return

    df = pd.DataFrame(records)
    conn = sqlite3.connect(db_name)
    try:
        df.to_sql("weather_data", conn, if_exists="append", index=False)
        print(f"Loaded {len(df)} records into 'weather_data'.")
    finally:
        conn.close()
