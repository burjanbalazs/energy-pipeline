# ingestion/producers/energy_producer.py
#
# Two modes:
#   --backfill
#       Pulls 2025-01-01 → today - 1 day (ENTSO-E lag is shorter than Open-Meteo).
#       Run once on setup.
#
#   --incremental
#       Pulls yesterday's finalised data.
#       Run daily via Airflow.
#
# Requires ENTSOE_API_KEY in environment.
# If key not present, logs a warning and exits gracefully.
from entsoe import EntsoeRawClient
import os
import config
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta, date
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("energy_producer")
client = EntsoeRawClient(api_key=os.getenv("ENTSOE_API_KEY"))


# ── Kafka ────────────────────────────────────────────────────
def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        "client.id":         "energy_producer",
        "retries":           5,
        "retry.backoff.ms":  1000,
        "acks":              "all",
    })

def get_aggregation_factor(resolution):
    if resolution == "PT60M":
        return 1
    if resolution == "PT30M":
        return 2
    if resolution == "PT15M":
        return 4

def delivery_callback(err, msg):
    if err:
        log.error(f"Message failed delivery: {err}")
    else:
        log.debug(f"Delivered → {msg.topic()} partition={msg.partition()} offset={msg.offset()}")

# ── Entrypoint ───────────────────────────────────────────────
if __name__ == "__main__":
    if not os.getenv("ENTSOE_API_KEY"):
        log.warning(
            "ENTSOE_API_KEY not set. "
            "Set the key in .env once your ENTSO-E token is approved."
        )
        raise SystemExit(0)
    
    producer = make_producer()
    start_date = pd.Timestamp(date.today() - timedelta(5)).tz_localize("UTC")
    end_date = pd.Timestamp(date.today() - timedelta(4)).tz_localize("UTC")

    for city, country in config.CITIES:
        xml_string = client.query_load(country_code="DE",start=start_date,end=end_date)
        ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
        tree = ET.fromstring(xml_string)

        for time_series in tree.findall('ns:TimeSeries', ns):
            for period in time_series.findall('ns:Period', ns):

                resolution = period.find('ns:resolution', ns).text
                time_interval = period.find('ns:timeInterval', ns)
                start_elem = time_interval.find('ns:start', ns)
                start_time = datetime.fromisoformat(start_elem.text.replace('Z', '+00:00'))
                
                count = get_aggregation_factor(resolution)
                i = 0
                sum = 0
                hour = 0
                
                for point in period.findall('ns:Point', ns):
                    quantity = float(point.find('ns:quantity', ns).text)
                    sum += quantity
                    i += 1
                    if i == count:
                        avg = sum / count
                        energy_message = {
                            "city_name": city,
                            "country": country,
                            "time": (start_time + timedelta(hours=hour)).isoformat(),
                            "load_mw": avg,
                        }
                        producer.produce(
                            topic="raw.energy.demand",
                            value=str(energy_message),
                            key=country,
                            callback=delivery_callback
                        )
                        i = 0
                        sum = 0
                        hour = hour + 1
    producer.flush()