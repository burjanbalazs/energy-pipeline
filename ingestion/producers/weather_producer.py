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

import requests
from confluent_kafka import Producer

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("weather_producer")

CITIES_LIST = [
    'Berlin',
    'Paris',
    'Rome',
    'London',
    'Madrid',
    'Warsaw',
    'Vienna',
    'Budapest',
    'Bucharest',
    'Lisbon'
]

OPENMETEO_GEO_API_URL = "https://geocoding-api.open-meteo.com/v1/search"
OPENMETEO_URL = 'https://archive-api.open-meteo.com/v1/archive'
OPENMETEO_VARIABLES = ["temperature_2m,relative_humidity_2m,"
    "wind_speed_10m,shortwave_radiation,"
    "precipitation,weather_code"]



def query_city_information(list_of_cities: None) -> list:
    cities_information = []
    for city in CITIES_LIST:
        param = {
            "name": city,
            "count": 1
        }
        city_information = requests.get(OPENMETEO_GEO_API_URL, params=param)
        city_information.raise_for_status()
        city_json = city_information.json()
        cities_information.append((city_json["results"][0]["country_code"], city_json["results"][0]["name"], city_json["results"][0]["latitude"], city_json["results"][0]["longitude"]))
    return cities_information
        



def run(date_start=None, date_end=None):
    cities_to_query = query_city_information(CITIES_LIST)
    

# ── Entrypoint ───────────────────────────────────────────────

if __name__ == "__main__":
    BACKFILL_END_DATE = date.today() - timedelta(5)
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('-b', '--backfill', help=f"This will backfill all the data from a specified date until {BACKFILL_END_DATE}")
    mode.add_argument('-i', '--incremental', action='store_true', help='This will run the program to get the data only on the day of {BACKFILL_END_DATE}')
    args = parser.parse_args()
    run(date_start=args.backfill, date_end=BACKFILL_END_DATE)