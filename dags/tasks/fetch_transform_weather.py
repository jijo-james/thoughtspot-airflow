from datetime import datetime, timedelta
from typing import Dict, Any
import requests


def fetch_transform_weather(
    weather_base_url: str, weather_api_key: str, location: str, days: int = 7
) -> Dict[str, Any]:
    """
    Fetch weather data from WeatherAPI for the past `days` days, transform, and return records.
    """
    end_date = datetime.today()
    start_date = end_date - timedelta(days=days)

    records = []

    for i in range(days):
        date_to_query = start_date + timedelta(days=i)
        date_str = date_to_query.strftime("%Y-%m-%d")

        params = {"key": weather_api_key, "q": location, "dt": date_str}

        # Fetch
        try:
            resp = requests.get(weather_base_url, params=params, timeout=60)
            resp.raise_for_status()
            weather_json = resp.json()
        except requests.RequestException as e:
            print(f"Error fetching weather data for {date_str}: {e}")
            continue

        # Transform
        if (
            "forecast" not in weather_json
            or "forecastday" not in weather_json["forecast"]
        ):
            print(f"Malformed weather data for {date_str}")
            continue

        daily_info = weather_json["forecast"]["forecastday"][0]["day"]
        record = {
            "date": date_str,
            "temperature": daily_info.get("avgtemp_c"),
            "humidity": daily_info.get("avghumidity"),
            "precipitation": daily_info.get("totalprecip_mm"),
            "wind_speed": daily_info.get("maxwind_kph"),
        }
        records.append(record)

    return {"records": records}
