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


def delivery_callback(err, msg):
    if err:
        log.error(f"Delivery failed for {msg.key()}: {err}")
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
    
    start_date = pd.Timestamp(date.today() - timedelta(5)).tz_localize("UTC")
    end_date = pd.Timestamp(date.today() - timedelta(4)).tz_localize("UTC")
    # xml_string = client.query_load(country_code="DE",start=start_date,end=end_date)
    ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
    # tree = ET.fromstring(xml_string)
    print(start_date + pd.DateOffset(days=1))
    # for time_series in tree.findall('ns:TimeSeries', ns):
    #     for period in time_series.findall('ns:Period', ns):
    #         for point in period.findall('ns:Point', ns):
    #             print(1)
    #             print(period.tag)