# tests/test_fetch_transform_weather.py

import unittest
from unittest.mock import patch, Mock
from tasks.fetch_transform_weather import fetch_transform_weather
import requests
from datetime import datetime, timedelta


class TestFetchTransformWeather(unittest.TestCase):
    def setUp(self):
        self.weather_base_url = "http://api.weatherapi.com/v1/history.json"
        self.weather_api_key = "demo"
        self.location = "New York"
        self.days = 7

        # Sample valid weather JSON response
        self.valid_weather_json = {
            "forecast": {
                "forecastday": [
                    {
                        "date": "2025-01-03",
                        "day": {
                            "avgtemp_c": 15.5,
                            "avghumidity": 70,
                            "totalprecip_mm": 5.2,
                            "maxwind_kph": 20,
                        },
                    }
                ]
            },
            "location": {
                "name": "New York",
                "region": "New York",
                "country": "USA",
            },
        }

        # Sample malformed weather JSON response (missing "forecastday")
        self.malformed_weather_json = {
            "forecast": {
                # "forecastday" key is missing
            },
            "location": {
                "name": "New York",
                "region": "New York",
                "country": "USA",
            },
        }

    @patch("tasks.fetch_transform_weather.requests.get")
    def test_fetch_transform_weather_success(self, mock_get):
        """
        Test that fetch_transform_weather successfully fetches and transforms weather data.
        """

        # Generate a list of valid responses for each day
        def side_effect(url, params, timeout, *args, **kwargs):
            date_str = params["dt"]
            mock_resp = Mock()
            mock_resp.raise_for_status.return_value = None
            mock_resp.json.return_value = {
                "forecast": {
                    "forecastday": [
                        {
                            "date": date_str,
                            "day": {
                                "avgtemp_c": 15.5,
                                "avghumidity": 70,
                                "totalprecip_mm": 5.2,
                                "maxwind_kph": 20,
                            },
                        }
                    ]
                },
                "location": {
                    "name": "New York",
                    "region": "New York",
                    "country": "USA",
                },
            }
            return mock_resp

        mock_get.side_effect = side_effect

        result = fetch_transform_weather(
            weather_base_url=self.weather_base_url,
            weather_api_key=self.weather_api_key,
            location=self.location,
            days=self.days,
        )

        self.assertIn("records", result)
        self.assertIsInstance(result["records"], list)
        self.assertEqual(len(result["records"]), self.days)

        # Check the structure of a single record
        record = result["records"][0]
        expected_keys = {
            "date",
            "temperature",
            "humidity",
            "precipitation",
            "wind_speed",
        }
        self.assertEqual(set(record.keys()), expected_keys)
        self.assertEqual(record["temperature"], 15.5)
        self.assertEqual(record["humidity"], 70)
        self.assertEqual(record["precipitation"], 5.2)
        self.assertEqual(record["wind_speed"], 20)

    @patch("tasks.fetch_transform_weather.requests.get")
    def test_fetch_transform_weather_request_exception(self, mock_get):
        """
        Test that fetch_transform_weather handles a RequestException gracefully.
        """
        mock_get.side_effect = requests.RequestException("Connection error")

        result = fetch_transform_weather(
            weather_base_url=self.weather_base_url,
            weather_api_key=self.weather_api_key,
            location=self.location,
            days=self.days,
        )

        self.assertIn("records", result)
        self.assertIsInstance(result["records"], list)
        self.assertEqual(len(result["records"]), 0)

    @patch("tasks.fetch_transform_weather.requests.get")
    def test_fetch_transform_weather_malformed_data(self, mock_get):
        """
        Test that fetch_transform_weather handles malformed JSON responses.
        """
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = self.malformed_weather_json
        mock_get.return_value = mock_response

        result = fetch_transform_weather(
            weather_base_url=self.weather_base_url,
            weather_api_key=self.weather_api_key,
            location=self.location,
            days=self.days,
        )

        self.assertIn("records", result)
        self.assertIsInstance(result["records"], list)
        self.assertEqual(len(result["records"]), 0)

    @patch("tasks.fetch_transform_weather.requests.get")
    def test_fetch_transform_weather_partial_malformed_data(self, mock_get):
        """
        Test that fetch_transform_weather handles a mix of valid and malformed data.
        """

        # First 5 days are valid, last 2 days are malformed
        def side_effect(url, params, timeout, *args, **kwargs):
            date_str = params["dt"]
            day_offset = (
                datetime.today() - datetime.strptime(date_str, "%Y-%m-%d")
            ).days
            if day_offset < 5:
                # Valid data for first 5 days (day_offset=0 to 4)
                mock_resp = Mock()
                mock_resp.raise_for_status.return_value = None
                mock_resp.json.return_value = {
                    "forecast": {
                        "forecastday": [
                            {
                                "date": date_str,
                                "day": {
                                    "avgtemp_c": 15.5,
                                    "avghumidity": 70,
                                    "totalprecip_mm": 5.2,
                                    "maxwind_kph": 20,
                                },
                            }
                        ]
                    },
                    "location": {
                        "name": "New York",
                        "region": "New York",
                        "country": "USA",
                    },
                }
                return mock_resp
            else:
                # Malformed data for last 2 days (day_offset=5 and 6)
                mock_resp = Mock()
                mock_resp.raise_for_status.return_value = None
                mock_resp.json.return_value = self.malformed_weather_json
                return mock_resp

        mock_get.side_effect = side_effect

        result = fetch_transform_weather(
            weather_base_url=self.weather_base_url,
            weather_api_key=self.weather_api_key,
            location=self.location,
            days=self.days,
        )

        self.assertIn("records", result)
        self.assertIsInstance(result["records"], list)
        self.assertEqual(len(result["records"]), 4)  # Only 5 valid days


if __name__ == "__main__":
    unittest.main()
