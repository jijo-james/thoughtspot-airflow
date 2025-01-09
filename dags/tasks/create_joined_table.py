import sqlite3


def create_joined_table(db_name: str) -> None:
    """
    Create a joined table from stock_data and weather_data in the SQLite DB.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS joined_data;")

        join_query = """
            CREATE TABLE joined_data AS
            SELECT
                s.date AS stock_date,
                s.open,
                s.high,
                s.low,
                s.close,
                s.volume,
                s.stock_symbol,
                s.partition_hour,
                w.temperature,
                w.humidity,
                w.precipitation,
                w.wind_speed
            FROM stock_data s
            JOIN weather_data w
            ON s.date = w.date;
        """
        cursor.execute(join_query)
        conn.commit()
        print("Created joined_data table.")
    except Exception as e:
        print(f"Error creating joined_data table: {e}")
    finally:
        cursor.close()
        conn.close()
