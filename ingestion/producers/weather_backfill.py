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


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        "client.id": "weather_producer",
        "retries": 5,
        "retry.backoff.ms": 1000,
        "acks": "all",
    })

def deliver_callback(err, msg):
    if err:
        log.error(f"Message failed delivery: {err}")
    else:
        log.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def get_weather_data(latitude: float, longitude: float, date_start: str, date_end: str):
    query_params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": date_start,
        "end_date": date_end,
        "hourly": config.OPENMETEO_VARIABLES
    }
    return requests.get(config.OPENMETEO_URL, params=query_params).json()

def query_city_information() -> list:
    """Query Open-Meteo geo API to get coordinates for each city."""
    cities_information = []
    for city_name, country_code in config.CITIES:
        param = {
            "name": city_name,
            "count": 1
        }
        city_information = requests.get(config.OPENMETEO_GEO_API_URL, params=param)
        city_information.raise_for_status()
        city_json = city_information.json()
        result = city_json["results"][0]
        cities_information.append((
            country_code,
            result["name"],
            result["latitude"],
            result["longitude"]
        ))
    return cities_information

def construct_weather_message(weather_data: dict, city_code: str, city_name: str, latitude: float, longitude: float, date_end: str) -> list[dict]:
    weather_message = []
    for index, record in enumerate(weather_data["hourly"]["time"]):
            object = {
                "city_name": city_name,
                "country": city_code,
                "latitude": latitude,
                "longitude": longitude,
                "time": record,
                "temperature": weather_data["hourly"]["temperature_2m"][index],
                "humidity": weather_data["hourly"]["relative_humidity_2m"][index],
                "wind_speed": weather_data["hourly"]["wind_speed_10m"][index],
                "precipitation": weather_data["hourly"]["precipitation"][index],
                "weather_code": weather_data["hourly"]["weather_code"][index],
                "weather": config.WEATHER_CODES[weather_data["hourly"]["weather_code"][index]]
            }
            weather_message.append(object)
    return weather_message

def get_data_backfill(city, date_start: str, date_end: str):
    (city_code, city_name, latitude, longitude) = city
    weather_data = get_weather_data(latitude, longitude, date_start, date_end)
    weather_message = construct_weather_message(weather_data, city_code, city_name, latitude, longitude, date_end)

    return weather_message

def run(date_start=None, date_end=None):
    producer = make_producer()
    cities_to_query = query_city_information()
    for city in cities_to_query:
        weather_messages = get_data_backfill(city, date_start, date_end)
        for message in weather_messages:
            producer.produce(config.KAFKA_TOPIC, callback=deliver_callback, key=city[1], value=str(message))
    producer.flush()

# ── Entrypoint ───────────────────────────────────────────────

if __name__ == "__main__":
    load_date = date.today() - timedelta(6)
    run(date_start=load_date-timedelta(days=365), date_end=load_date)