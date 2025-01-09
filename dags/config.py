import os
import json
from pydantic import BaseModel


class Constants(BaseModel):
    STOCK_BASE_URL: str
    FUNCTION: str
    SYMBOL: str
    OUTPUTSIZE: str
    WEATHER_BASE_URL: str
    LOCATION: str
    STOCK_API_KEY: str
    WEATHER_API_KEY: str
    DB_NAME: str


def load_config() -> Constants:
    # Adjust the path to your local config.json
    dir_path = os.path.dirname(os.path.realpath(__file__))
    file_path = f"{dir_path}/config.json"
    with open(file_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    return Constants(**config)
