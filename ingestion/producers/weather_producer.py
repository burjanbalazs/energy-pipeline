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

from ingestion.schemas.weather_schema import WeatherMessage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("weather_producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = "raw.weather.hourly"

ARCHIVE_LAG_DAYS = 5
BACKFILL_START   = date(2025, 1, 1)

CITIES = [
    ("DE", "Berlin",    52.52,  13.41),
    ("FR", "Paris",     48.85,   2.35),
    ("ES", "Madrid",    40.42,  -3.70),
    ("IT", "Rome",      41.90,  12.49),
    ("PL", "Warsaw",    52.23,  21.01),
    ("NL", "Amsterdam", 52.37,   4.90),
    ("BE", "Brussels",  50.85,   4.35),
    ("AT", "Vienna",    48.21,  16.37),
    ("CH", "Zurich",    47.38,   8.54),
    ("CZ", "Prague",    50.08,  14.44),
]

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
ARCHIVE_VARIABLES = (
    "temperature_2m,relative_humidity_2m,"
    "wind_speed_10m,shortwave_radiation,"
    "precipitation,weather_code"
)


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "weather_producer",
        "retries": 5,
        "retry.backoff.ms": 1000,
        "acks": "all",
    })


def delivery_callback(err, msg):
    if err:
        log.error(f"Delivery failed for {msg.key()}: {err}")
    else:
        log.debug(f"Delivered → {msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def fetch_historical(lat: float, lon: float, start: date, end: date) -> dict:
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": start.isoformat(),
        "end_date":   end.isoformat(),
        "hourly":     ARCHIVE_VARIABLES,
        "timezone":   "UTC",
    }
    resp = requests.get(ARCHIVE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def parse_hourly_records(data: dict, city_code: str, city_name: str,
                          lat: float, lon: float) -> list[WeatherMessage]:
    ingested_at = datetime.now(timezone.utc)
    times = data["hourly"]["time"]
    records = []
    skipped = 0

    for i, ts in enumerate(times):
        try:
            msg = WeatherMessage(
                city_code=city_code,
                city_name=city_name,
                latitude=lat,
                longitude=lon,
                timestamp=ts,
                temperature_c=         data["hourly"]["temperature_2m"][i],
                relative_humidity_pct= data["hourly"]["relative_humidity_2m"][i],
                wind_speed_kmh=        data["hourly"]["wind_speed_10m"][i],
                solar_radiation_wm2=   data["hourly"]["shortwave_radiation"][i],
                precipitation_mm=      data["hourly"]["precipitation"][i],
                weather_code=          data["hourly"]["weather_code"][i],
                ingested_at=ingested_at,
            )
            records.append(msg)
        except Exception as e:
            skipped += 1
            log.warning(f"Skipping invalid record at {ts} for {city_name}: {e}")

    if skipped:
        log.warning(f"{city_name}: skipped {skipped}/{len(times)} records")

    return records


def publish_records(producer: Producer, records: list[WeatherMessage], city_code: str):
    for record in records:
        producer.produce(
            topic=TOPIC,
            key=city_code,
            value=record.model_dump_json(),
            callback=delivery_callback,
        )
    producer.flush(timeout=60)


def date_chunks(start: date, end: date, chunk_days: int = 90):
    """
    Yields (chunk_start, chunk_end) pairs.
    90-day chunks over ~14 months is fast and gives readable progress logs.
    """
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


# ── Modes ────────────────────────────────────────────────────

def run_backfill():
    start = BACKFILL_START
    end   = date.today() - timedelta(days=ARCHIVE_LAG_DAYS)

    producer = make_producer()
    log.info(f"Backfill: {start} → {end} for {len(CITIES)} cities")

    for city_code, city_name, lat, lon in CITIES:
        log.info(f"Processing {city_name} ({city_code})...")
        total = 0

        for chunk_start, chunk_end in date_chunks(start, end):
            try:
                log.info(f"  Fetching {chunk_start} → {chunk_end}")
                data    = fetch_historical(lat, lon, chunk_start, chunk_end)
                records = parse_hourly_records(data, city_code, city_name, lat, lon)
                publish_records(producer, records, city_code)
                total  += len(records)
                log.info(f"  Published {len(records)} records")
                time.sleep(1)   # polite pause between chunks on the free API

            except requests.RequestException as e:
                log.error(f"  HTTP error {chunk_start}→{chunk_end}: {e}")
            except Exception as e:
                log.error(f"  Unexpected error: {e}", exc_info=True)

        log.info(f"{city_name} done — {total} total records")

    log.info("Backfill complete")


def run_incremental():
    target_date = date.today() - timedelta(days=ARCHIVE_LAG_DAYS)
    producer    = make_producer()
    log.info(f"Incremental run for {target_date}")

    success, errors = 0, 0

    for city_code, city_name, lat, lon in CITIES:
        try:
            data    = fetch_historical(lat, lon, target_date, target_date)
            records = parse_hourly_records(data, city_code, city_name, lat, lon)
            publish_records(producer, records, city_code)
            success += 1
            log.info(f"  {city_name} ({city_code}) — {len(records)} hours published")

        except requests.RequestException as e:
            log.error(f"HTTP error for {city_name}: {e}")
            errors += 1
        except Exception as e:
            log.error(f"Unexpected error for {city_name}: {e}", exc_info=True)
            errors += 1

    log.info(f"Incremental done — {success} cities ok, {errors} errors")


# ── Entrypoint ───────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--backfill",    action="store_true",
                      help=f"Pull {BACKFILL_START} → today minus {ARCHIVE_LAG_DAYS} days")
    mode.add_argument("--incremental", action="store_true",
                      help="Pull most recent finalised day (for Airflow)")
    args = parser.parse_args()

    if args.backfill:
        run_backfill()
    else:
        run_incremental()