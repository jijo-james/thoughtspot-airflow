import unittest
from unittest.mock import patch, Mock
import math
import requests

from tasks.fetch_transform_stock import fetch_transform_stock


class TestFetchTransformStock(unittest.TestCase):
    def setUp(self):
        self.stock_base_url = "https://www.alphavantage.co/query"
        self.stock_api_key = "demo"
        self.function = "TIME_SERIES_DAILY"
        self.symbol = "IBM"
        self.outputsize = "compact"
        self.days_to_keep = 7

        self.valid_stock_json = {
            "Meta Data": {
                "2. Symbol": "IBM",
                "1. Information": "Daily Time Series with Splits and Dividend Events",
                "3. Last Refreshed": "2023-10-10",
                "4. Output Size": "Compact",
                "5. Time Zone": "US/Eastern",
            },
            "Time Series (Daily)": {
                "2023-10-10": {
                    "1. open": "140.0000",
                    "2. high": "142.0000",
                    "3. low": "139.0000",
                    "4. close": "141.0000",
                    "5. volume": "3000000",
                },
                "2023-10-09": {
                    "1. open": "138.0000",
                    "2. high": "141.0000",
                    "3. low": "137.5000",
                    "4. close": "140.5000",
                    "5. volume": "2500000",
                },
            },
        }

        self.malformed_stock_json = {
            "Meta Data": {
                "2. Symbol": "IBM",
                "1. Information": "Daily Time Series with Splits and Dividend Events",
                "3. Last Refreshed": "2023-10-10",
                "4. Output Size": "Compact",
                "5. Time Zone": "US/Eastern",
            }
        }

    @patch("tasks.fetch_transform_stock.requests.get")
    def test_fetch_transform_stock_success(self, mock_get):
        """
        Test that fetch_transform_stock successfully fetches and transforms stock data.
        """
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = self.valid_stock_json
        mock_get.return_value = mock_response

        result = fetch_transform_stock(
            stock_base_url=self.stock_base_url,
            stock_api_key=self.stock_api_key,
            function=self.function,
            symbol=self.symbol,
            outputsize=self.outputsize,
            days_to_keep=self.days_to_keep,
        )

        self.assertIn("records", result)
        self.assertIsInstance(result["records"], list)
        self.assertEqual(
            len(result["records"]), len(self.valid_stock_json["Time Series (Daily)"])
        )

        record = result["records"][0]
        expected_keys = {
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "stock_symbol",
            "partition_hour",
        }
        self.assertEqual(set(record.keys()), expected_keys)
        self.assertEqual(record["stock_symbol"], "IBM")

    @patch("tasks.fetch_transform_stock.requests.get")
    def test_fetch_transform_stock_request_exception(self, mock_get):
        """
        Test that fetch_transform_stock handles a RequestException gracefully.
        """
        mock_get.side_effect = requests.RequestException("Connection error")

        result = fetch_transform_stock(
            stock_base_url=self.stock_base_url,
            stock_api_key=self.stock_api_key,
            function=self.function,
            symbol=self.symbol,
            outputsize=self.outputsize,
            days_to_keep=self.days_to_keep,
        )

        self.assertIn("records", result)
        self.assertIsInstance(result["records"], list)
        self.assertEqual(len(result["records"]), 0)

    @patch("tasks.fetch_transform_stock.requests.get")
    def test_fetch_transform_stock_malformed_data(self, mock_get):
        """
        Test that fetch_transform_stock handles malformed JSON responses.
        """
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = self.malformed_stock_json
        mock_get.return_value = mock_response

        result = fetch_transform_stock(
            stock_base_url=self.stock_base_url,
            stock_api_key=self.stock_api_key,
            function=self.function,
            symbol=self.symbol,
            outputsize=self.outputsize,
            days_to_keep=self.days_to_keep,
        )

        self.assertIn("records", result)
        self.assertIsInstance(result["records"], list)
        self.assertEqual(len(result["records"]), 0)

    @patch("tasks.fetch_transform_stock.requests.get")
    def test_fetch_transform_stock_invalid_numeric_values(self, mock_get):
        """
        Test that fetch_transform_stock handles non-numeric values gracefully.
        """
        invalid_stock_json = self.valid_stock_json.copy()
        invalid_stock_json["Time Series (Daily)"]["2023-10-10"][
            "1. open"
        ] = "invalid_number"

        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = invalid_stock_json
        mock_get.return_value = mock_response

        result = fetch_transform_stock(
            stock_base_url=self.stock_base_url,
            stock_api_key=self.stock_api_key,
            function=self.function,
            symbol=self.symbol,
            outputsize=self.outputsize,
            days_to_keep=self.days_to_keep,
        )

        self.assertIn("records", result)
        self.assertIsInstance(result["records"], list)
        self.assertEqual(
            len(result["records"]), len(invalid_stock_json["Time Series (Daily)"])
        )

        record = result["records"][0]
        self.assertIn("open", record)
        self.assertTrue(
            math.isnan(record["open"]),
            "Expected 'open' to be NaN for invalid numeric value",
        )


if __name__ == "__main__":
    unittest.main()
