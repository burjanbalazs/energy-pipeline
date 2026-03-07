# ingestion/producers/weather_producer.py
#
# Two modes:
#   --backfill
#       Pulls 2025-01-01 → today - 5 days. Run once on setup.
#
#   --incremental
#       Pulls the most recently finalised day (today - 5 days).
#       Run daily via Airflow.

import os
import time
import logging
import argparse
from datetime import datetime, timezone, timedelta, date
import config
import requests
from confluent_kafka import Producer

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("weather_producer")

def query_city_information(list_of_cities: None) -> list:
    cities_information = []
    for city in config.CITIES_LIST:
        param = {
            "name": city,
            "count": 1
        }
        city_information = requests.get(config.OPENMETEO_GEO_API_URL, params=param)
        city_information.raise_for_status()
        city_json = city_information.json()
        cities_information.append((city_json["results"][0]["country_code"], city_json["results"][0]["name"], city_json["results"][0]["latitude"], city_json["results"][0]["longitude"]))
    return cities_information

def get_data_incremental(cities_to_query: list, date_end: str):
    for city in cities_to_query:
        (city_code, city_name, latitude, longitude) = city
        query_params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": date_end,
            "end_date": date_end,
            "hourly": config.OPENMETEO_VARIABLES
        }
        resp = requests.get(config.OPENMETEO_URL, params=query_params)
        response_json = resp.json()
        weather_message = []
        for index, record in enumerate(response_json["hourly"]["time"]):
            object = {
                "city_name": city_name,
                "city": city_code,
                "latitude": latitude,
                "longitude": longitude,
                "time": record,
                "temperature": response_json["hourly"]["temperature_2m"][index],
                "humidity": response_json["hourly"]["relative_humidity_2m"][index],
                "wind_speed": response_json["hourly"]["wind_speed_10m"][index],
                "precipitation": response_json["hourly"]["precipitation"][index],
                "weather_code": response_json["hourly"]["weather_code"][index],
                "weather": config.WEATHER_CODES[response_json["hourly"]["weather_code"][index]]
            }
            weather_message.append(object)
        return weather_message

def run(date_start=None, date_end=None):
    cities_to_query = query_city_information(config.CITIES_LIST)
    if date_start == None:
        get_data_incremental(cities_to_query, date_end)
    

# ── Entrypoint ───────────────────────────────────────────────

if __name__ == "__main__":
    BACKFILL_END_DATE = date.today() - timedelta(5)
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('-b', '--backfill', help=f"This will backfill all the data from a specified date until {BACKFILL_END_DATE}")
    mode.add_argument('-i', '--incremental', action='store_true', help='This will run the program to get the data only on the day of {BACKFILL_END_DATE}')
    args = parser.parse_args()
    run(date_start=args.backfill, date_end=BACKFILL_END_DATE)