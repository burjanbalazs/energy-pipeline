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

import os
import time
import logging
import argparse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta, date

import requests
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("energy_producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
ENTSOE_API_KEY          = os.getenv("ENTSOE_API_KEY")
TOPIC                   = "raw.energy.demand"

# ENTSO-E publishes actual load data with roughly a 1-day lag
ARCHIVE_LAG_DAYS = 1
BACKFILL_START   = date(2025, 1, 1)

ENTSOE_URL = "https://web-api.tp.entsoe.eu/api"

BIDDING_ZONES = {
    "DE": "10Y1001A1001A83F",
    "FR": "10YFR-RTE------C",
    "ES": "10YES-REE------0",
    "IT": "10YIT-GRTN-----B",
    "PL": "10YPL-AREA-----S",
    "NL": "10YNL----------L",
    "BE": "10YBE----------2",
    "AT": "10YAT-APG------L",
    "CH": "10YCH-SWISSGRIDZ",
    "CZ": "10YCZ-CEPS-----N",
}


# ── Kafka ────────────────────────────────────────────────────
def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
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


# ── API ──────────────────────────────────────────────────────
def fetch_load(bidding_zone: str, start: date, end: date) -> str:
    """
    Fetch actual total load from ENTSO-E for a date range.
    Returns raw XML — ENTSO-E doesn't offer JSON.
    """
    params = {
        "securityToken":         ENTSOE_API_KEY,
        "documentType":          "A65",    # system total load
        "processType":           "A16",    # realised (actual, not forecast)
        "outBiddingZone_Domain": bidding_zone,
        "periodStart":           start.strftime("%Y%m%d0000"),
        "periodEnd":             end.strftime("%Y%m%d2300"),
    }
    resp = requests.get(ENTSOE_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.text


def parse_entsoe_xml(xml_text: str, country_code: str, bidding_zone: str) -> list[dict]:
    """
    ENTSO-E returns XML with nested TimeSeries → Period → Point structure.
    We flatten it into one dict per hour.
    """
    ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
    root = ET.fromstring(xml_text)
    results = []

    for time_series in root.findall("ns:TimeSeries", ns):
        for period in time_series.findall("ns:Period", ns):
            start_str  = period.find("ns:timeInterval/ns:start", ns).text
            resolution = period.find("ns:resolution", ns).text
            start_dt   = datetime.strptime(start_str, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)

            for point in period.findall("ns:Point", ns):
                position = int(point.find("ns:position", ns).text)
                quantity = float(point.find("ns:quantity", ns).text)
                # position is 1-based, each step is 1 hour for PT60M resolution
                point_start = start_dt + timedelta(hours=position - 1)
                point_end   = point_start + timedelta(hours=1)

                results.append({
                    "country_code": country_code,
                    "bidding_zone":  bidding_zone,
                    "timestamp":     point_start,
                    "period_end":    point_end,
                    "load_mw":       quantity,
                    "resolution":    resolution,
                })

    return results


def publish_records(producer: Producer, records: list[dict], country_code: str,
                    ingested_at: datetime):
    skipped = 0
    for record in records:
        try:
            message = EnergyMessage(ingested_at=ingested_at, **record)
            producer.produce(
                topic=TOPIC,
                key=country_code,
                value=message.model_dump_json(),
                callback=delivery_callback,
            )
        except Exception as e:
            skipped += 1
            log.warning(f"Skipping invalid record {record.get('timestamp')}: {e}")

    producer.flush(timeout=60)
    if skipped:
        log.warning(f"{country_code}: skipped {skipped}/{len(records)} records")


def date_chunks(start: date, end: date, chunk_days: int = 30):
    """
    Yields (chunk_start, chunk_end) pairs.
    ENTSO-E has stricter rate limits than Open-Meteo so we
    use 30-day chunks and pause between requests.
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
    log.info(f"Backfill: {start} → {end} for {len(BIDDING_ZONES)} countries")

    for country_code, bidding_zone in BIDDING_ZONES.items():
        log.info(f"Processing {country_code}...")
        total = 0

        for chunk_start, chunk_end in date_chunks(start, end):
            try:
                log.info(f"  Fetching {chunk_start} → {chunk_end}")
                xml_text   = fetch_load(bidding_zone, chunk_start, chunk_end)
                records    = parse_entsoe_xml(xml_text, country_code, bidding_zone)
                ingested_at = datetime.now(timezone.utc)
                publish_records(producer, records, country_code, ingested_at)
                total += len(records)
                log.info(f"  Published {len(records)} records")
                # ENTSO-E rate limits — be conservative
                time.sleep(2)

            except requests.RequestException as e:
                log.error(f"  HTTP error {chunk_start}→{chunk_end}: {e}")
                time.sleep(5)   # back off on errors
            except Exception as e:
                log.error(f"  Unexpected error: {e}", exc_info=True)

        log.info(f"{country_code} done — {total} total records")

    log.info("Backfill complete")


def run_incremental():
    target_date = date.today() - timedelta(days=ARCHIVE_LAG_DAYS)
    producer    = make_producer()
    log.info(f"Incremental run for {target_date}")

    success, errors = 0, 0

    for country_code, bidding_zone in BIDDING_ZONES.items():
        try:
            xml_text    = fetch_load(bidding_zone, target_date, target_date)
            records     = parse_entsoe_xml(xml_text, country_code, bidding_zone)
            ingested_at = datetime.now(timezone.utc)
            publish_records(producer, records, country_code, ingested_at)
            success += 1
            log.info(f"  {country_code} — {len(records)} hours published")
            time.sleep(1)

        except requests.RequestException as e:
            log.error(f"HTTP error for {country_code}: {e}")
            errors += 1
        except Exception as e:
            log.error(f"Unexpected error for {country_code}: {e}", exc_info=True)
            errors += 1

    log.info(f"Incremental done — {success} countries ok, {errors} errors")


# ── Entrypoint ───────────────────────────────────────────────
if __name__ == "__main__":
    if not ENTSOE_API_KEY:
        log.warning(
            "ENTSOE_API_KEY not set. "
            "Set the key in .env once your ENTSO-E token is approved."
        )
        raise SystemExit(0)

    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--backfill",    action="store_true",
                      help=f"Pull {BACKFILL_START} → today minus {ARCHIVE_LAG_DAYS} day")
    mode.add_argument("--incremental", action="store_true",
                      help="Pull most recent finalised day (for Airflow)")
    args = parser.parse_args()

    if args.backfill:
        run_backfill()
    else:
        run_incremental()