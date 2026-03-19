# ingestion/consumers/weather_consumer.py
#
# Reads from raw.weather.hourly and writes batches of messages
# to Azure Blob Storage (Bronze layer), partitioned by date.
#
# Run:  python -m ingestion.consumers.weather_consumer

import os
import json
import logging
from datetime import datetime, timezone
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("weather_consumer")

# ── Config ───────────────────────────────────────────────────
def run():
    return 1
if __name__ == "__main__":
    run()