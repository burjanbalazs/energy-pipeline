# ingestion/producers/energy_backfill.py
#
# Backfill mode: pulls energy demand data for the last year.
# Pulls historical data from ENTSOE-E API and publishes to Kafka.
#
# Run: python ingestion/producers/energy_backfill.py
#
# Requires ENTSOE_API_KEY in environment.
# If key not present, logs a warning and exits gracefully.

from entsoe import EntsoeRawClient
import os
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta, date
import pandas as pd
from confluent_kafka import Producer
from dotenv import load_dotenv
import config

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("energy_backfill")

# ENTSOE namespace for parsing XML responses
ENTSOE_NS = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}

# ── Kafka ────────────────────────────────────────────────────

def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        "client.id":         "energy_backfill",
        "retries":           5,
        "retry.backoff.ms":  1000,
        "acks":              "all",
    })


def delivery_callback(err, msg):
    if err:
        log.error(f"Delivery failed for {msg.key()}: {err}")
    else:
        log.info(f"Delivered → {msg.topic()} partition={msg.partition()} offset={msg.offset()}")


# ── ENTSOE API ────────────────────────────────────────────────

def get_energy_data(client: EntsoeRawClient, country_code: str, start_date: pd.Timestamp, end_date: pd.Timestamp) -> str:
    """
    Query ENTSOE-E API for load (demand) data.
    Returns XML string response.
    """
    try:
        xml_response = client.query_load(
            country_code=country_code,
            start=start_date,
            end=end_date
        )
        return xml_response
    except Exception as e:
        log.error(f"Error querying ENTSOE-E API for {country_code}: {e}")
        return None


def get_points_per_hour(resolution_str: str) -> int:
    """
    Parse ENTSOE-E resolution string and return how many points are in 1 hour.
    Examples: "PT15M" -> 4, "PT30M" -> 2, "PT60M" -> 1
    """
    if not resolution_str.startswith("PT") or not resolution_str.endswith("M"):
        log.warning(f"Unexpected resolution format: {resolution_str}, assuming PT60M")
        return 1

    minutes = int(resolution_str[2:-1])
    return 60 // minutes


def parse_energy_message(xml_string: str, country_code: str, city: str) -> list[dict]:
    """
    Parse ENTSOE-E XML response and extract load (demand) data.
    Aggregates sub-hourly data to hourly averages in a single pass over points.
    Returns list of message dicts ready for Kafka (one per hour).
    """
    messages = []

    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError as e:
        log.error(f"Failed to parse XML for {country_code}: {e}")
        return messages

    for time_series in root.findall('ns:TimeSeries', ENTSOE_NS):
        for period in time_series.findall('ns:Period', ENTSOE_NS):
            time_interval = period.find('ns:timeInterval', ENTSOE_NS)
            if time_interval is None:
                continue

            start_elem = time_interval.find('ns:start', ENTSOE_NS)
            if start_elem is None:
                continue

            resolution_elem = period.find('ns:resolution', ENTSOE_NS)
            resolution_str = resolution_elem.text if resolution_elem is not None else "PT60M"
            points_per_hour = get_points_per_hour(resolution_str)
            log.debug(f"Resolution: {resolution_str}, points per hour: {points_per_hour}")

            start_time = datetime.fromisoformat(start_elem.text.replace('Z', '+00:00'))

            # Single pass: running average over the points for each hour bucket
            hour_idx = 0
            running_sum = 0.0
            count = 0

            for point in period.findall('ns:Point', ENTSOE_NS):
                quantity = point.find('ns:quantity', ENTSOE_NS)
                if quantity is None:
                    continue

                try:
                    running_sum += float(quantity.text)
                    count += 1
                except (ValueError, TypeError):
                    log.warning(f"Failed to parse quantity for {country_code}, skipping point")
                    continue

                if count == points_per_hour:
                    messages.append({
                        "city_name": city,
                        "country": country_code,
                        "time": (start_time + timedelta(hours=hour_idx)).isoformat(),
                        "load_mw": round(running_sum / count, 2),
                    })
                    hour_idx += 1
                    running_sum = 0.0
                    count = 0

    return messages


def run(date_start: str, date_end: str):
    """
    Backfill energy demand data from date_start to date_end.
    Publishes messages to Kafka.
    """
    api_key = os.getenv("ENTSOE_API_KEY")
    if not api_key:
        log.warning(
            "ENTSOE_API_KEY not set. "
            "Set the key in .env once your ENTSO-E token is approved."
        )
        raise SystemExit(0)

    client = EntsoeRawClient(api_key=api_key)
    producer = make_producer()

    # Convert date strings to pandas Timestamps with UTC timezone
    if isinstance(date_start, date):
        date_start = pd.Timestamp(date_start).tz_localize("UTC")
    else:
        date_start = pd.Timestamp(date_start).tz_localize("UTC")

    if isinstance(date_end, date):
        date_end = pd.Timestamp(date_end).tz_localize("UTC")
    else:
        date_end = pd.Timestamp(date_end).tz_localize("UTC")

    log.info(f"Starting backfill from {date_start} to {date_end}")

    total_messages = 0

    # Backfill for each country (deduplicated from cities)
    countries_seen = set()
    for city_name, country_code in config.CITIES:
        if country_code in countries_seen:
            continue
        countries_seen.add(country_code)
        log.info(f"Fetching data for {country_code}...")

        # Query ENTSOE-E API
        xml_response = get_energy_data(client, country_code, date_start, date_end)
        if not xml_response:
            log.warning(f"No data returned for {country_code}")
            continue

        # Parse XML and extract messages
        messages = parse_energy_message(xml_response, country_code, city_name)
        log.info(f"Parsed {len(messages)} records for {country_code}")

        # Publish to Kafka
        for message in messages:
            producer.produce(
                "raw.energy.demand",
                callback=delivery_callback,
                key=country_code,
                value=str(message)
            )
            total_messages += 1

    producer.flush()
    log.info(f"Backfill complete. Published {total_messages} messages to Kafka.")


# ── Entrypoint ───────────────────────────────────────────────

if __name__ == "__main__":
    # Backfill for the last year
    # ENTSOE-E data typically has a 1-day lag
    end_date = date.today() - timedelta(days=6)
    start_date = end_date - timedelta(days=365)

    log.info(f"Backfilling energy demand data from {start_date} to {end_date}")
    run(date_start=start_date, date_end=end_date)
