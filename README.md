## Thoughtspot - Assignment

# Data Pipeline DAG with Apache Airflow

## Overview

This project implements an **Apache Airflow DAG** that automates the process of fetching, transforming, and loading data from two external APIs into a SQLite database. Specifically, the DAG performs the following tasks:

1. **Fetches Stock Data:** Retrieves daily stock data for a specified symbol from the [Alpha Vantage API](https://www.alphavantage.co/documentation/).
2. **Fetches Weather Data:** Retrieves historical weather data for a specified location from the [WeatherAPI](https://www.weatherapi.com/docs/).
3. **Transforms Data:** Utilizes Pandas to process and prepare the fetched data for storage.
4. **Loads Data into SQLite:** Inserts the transformed data into two separate tables (`stock_data` and `weather_data`) within a SQLite database.
5. **Creates a Joined Table:** Combines data from both tables into a new `joined_data` table for integrated analysis.

- Language: Python3.12
- DB: SQLite
- Test: Unittest
- Formatter: Black
- Linter: Pylint

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)


## Features

- **Automated Data Ingestion:** Seamlessly fetches data from external APIs on a scheduled basis.
- **Data Transformation:** Cleans and structures data using Pandas for efficient storage and analysis.
- **SQLite Integration:** Stores data in a lightweight, file-based SQLite database.
- **Data Integration:** Combines stock and weather data into a unified table for comprehensive insights.
- **Resilient Workflow:** Implements retry mechanisms to handle transient failures during data fetching.

## Prerequisites

Before setting up and running the DAG, ensure you have the following:

- **Python 3.12:** The script is configured to work with Python 3.12. Ensure you have this version installed.
- **Apache Airflow 2.10.4:** Installed with the Celery executor for distributed task management.
- **API Keys:**
  - **Alpha Vantage API Key:** Obtain from [Alpha Vantage](https://www.alphavantage.co/support/#api-key).
  - **WeatherAPI Key:** Obtain from [WeatherAPI](https://www.weatherapi.com/signup.aspx).
  (For convenience added the API keys in the [config.json] file)
- **SQLite:** No separate installation is required as SQLite comes bundled with Python.


## Installation
Run the following commands (Recommended to run in a virtualenviornment)
- pip install -r requirements.txt
- pip install -r requirements-airflow.txt

## Configuration

Add DAG path to the [airflow.config] file (Use absolute path)
Run the following commands
- airflow db init
- airflow webserver --port 8080
- airflow scheduler (Recommented to run in a different terminal)

## Usage
Go to the web browser and localhost:8080 (tada... Airflow is up and running...)
