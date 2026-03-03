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
KAFKA_BOOTSTRAP_SERVERS     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC                       = "raw.weather.hourly"
CONSUMER_GROUP              = "weather-bronze-writer"

AZURE_STORAGE_ACCOUNT_NAME  = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_ACCOUNT_KEY   = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
BRONZE_CONTAINER            = "bronze"

# How many messages to accumulate before flushing to blob storage.
# Keeps the number of small files low — one blob per batch per date partition.
BATCH_SIZE = 100


# ── Azure ────────────────────────────────────────────────────
def make_blob_client() -> BlobServiceClient:
    account_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
    return BlobServiceClient(
        account_url=account_url,
        credential=AZURE_STORAGE_ACCOUNT_KEY,
    )


def blob_path(record: dict, batch_id: str) -> str:
    """
    Builds the partitioned path for a record.
    e.g. weather/year=2025/month=01/day=15/DE_20250115T1400_abc123.json
    Partitioning by date makes PySpark reads efficient — you can
    filter to a date range without scanning the whole container.
    """
    ts = datetime.fromisoformat(record["timestamp"])
    return (
        f"weather/"
        f"year={ts.year}/"
        f"month={ts.month:02d}/"
        f"day={ts.day:02d}/"
        f"{record['city_code']}_{ts.strftime('%Y%m%dT%H%M')}_{batch_id}.json"
    )


def write_batch_to_blob(client: BlobServiceClient, records: list[dict]):
    """
    Groups records by their date partition and writes one blob per group.
    This avoids creating thousands of tiny single-record files.
    """
    # Group by date partition key
    partitions: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        ts = datetime.fromisoformat(record["timestamp"])
        partition_key = f"{ts.year}-{ts.month:02d}-{ts.day:02d}"
        partitions[partition_key].append(record)

    batch_id = datetime.now(timezone.utc).strftime("%H%M%S%f")
    container = client.get_container_client(BRONZE_CONTAINER)

    for partition_key, partition_records in partitions.items():
        # Use the first record to build the path
        path = blob_path(partition_records[0], batch_id)
        # Each line is a JSON object — newline-delimited JSON (NDJSON)
        # PySpark reads this format natively with spark.read.json()
        content = "\n".join(json.dumps(r) for r in partition_records)

        container.upload_blob(
            name=path,
            data=content.encode("utf-8"),
            overwrite=True,
        )
        log.info(f"Written {len(partition_records)} records → bronze/{path}")


# ── Consumer loop ────────────────────────────────────────────
def run():
    if not AZURE_STORAGE_ACCOUNT_NAME or not AZURE_STORAGE_ACCOUNT_KEY:
        raise EnvironmentError(
            "AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY must be set in .env"
        )

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "group.id":           CONSUMER_GROUP,
        "auto.offset.reset":  "earliest",   # start from beginning if no offset stored
        "enable.auto.commit": False,         # we commit manually after successful blob write
    })
    consumer.subscribe([TOPIC])

    blob_client = make_blob_client()
    batch: list[dict] = []

    log.info(f"Started — consuming {TOPIC}, writing to bronze container")

    try:
        while True:
            msg = consumer.poll(timeout=5.0)

            if msg is None:
                # No new messages — flush whatever is in the buffer
                if batch:
                    log.info(f"No new messages — flushing {len(batch)} buffered records")
                    write_batch_to_blob(blob_client, batch)
                    consumer.commit()
                    batch.clear()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.info(f"Reached end of partition {msg.partition()}")
                else:
                    log.error(f"Kafka error: {msg.error()}")
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))
                batch.append(record)
            except json.JSONDecodeError as e:
                log.error(f"Failed to decode message: {e}")
                continue

            # Flush when batch is full
            if len(batch) >= BATCH_SIZE:
                write_batch_to_blob(blob_client, batch)
                consumer.commit()   # only commit after successful write
                batch.clear()

    except KeyboardInterrupt:
        log.info("Shutting down — flushing remaining records")
        if batch:
            write_batch_to_blob(blob_client, batch)
            consumer.commit()
    finally:
        consumer.close()
        log.info("Consumer closed")


if __name__ == "__main__":
    run()