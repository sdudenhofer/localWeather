import streamlit as sl
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
import duckdb
from dotenv import load_dotenv
import os
from loguru import logger
import luigi
import datetime as dt

load_dotenv()
api_key = os.getenv('API')
logger.add("logs/logging.log", backtrace=True, diagnose=True)

# Connect to motherduck online to store data
con = duckdb.connect(f'md:?motherduck_token={api_key}')
today = dt.datetime.now()

class FetchAPIData(luigi.Task):
    def run(self):
        cached_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cached_session, retries = 5, backoff_factor = 0.2)
        openmeteo = openmeteo_requests.Client(session = retry_session)

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 44.0462, # this needs modified for your location
            "longitude": -123.022, # this needs modified for your location
            "hourly": "temperature_2m"
        }

        try:
            responses = openmeteo.weather_api(url, params=params)
        except TimeoutError as te:
            logger.exception(te)

        response = responses[0]
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

        hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit="s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc=True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
        )}

        hourly_data['temperature_2m'] = hourly_temperature_2m
        hourly_data['temperature_2m'].round()
        hourly_dataframe = pd.DataFrame(data = hourly_data)
        return hourly_dataframe
        try:
            con.sql('CREATE TABLE weather_data.hourly_data AS SELECT * FROM hourly_dataframe')
            logger.success(f'Table successfully created and populated - {today}')
        except Exception as E:
            logger.exception(E)
    def output(self):
        logger.success(f'Task completed - {today}')

if __name__ == '__main__':
    luigi.run()