from typing import Dict, Any
import sqlite3
import pandas as pd


def load_stock_data(db_name: str, stock_records: Dict[str, Any]) -> None:
    """
    Load stock data into SQLite (table: stock_data).
    """
    records = stock_records.get("records", [])
    if not records:
        print("No stock records to load.")
        return

    df = pd.DataFrame(records)
    conn = sqlite3.connect(db_name)
    try:
        df.to_sql("stock_data", conn, if_exists="append", index=False)
        print(f"Loaded {len(df)} records into 'stock_data'.")
    finally:
        conn.close()
