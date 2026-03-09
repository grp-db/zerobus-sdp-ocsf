"""
Zerobus producer: polls the GitHub Public Events API and streams each event
into the bronze Delta table via the Databricks Zerobus Ingest SDK (JSON mode).

push_zerobus.py extracts the event timestamp from created_at and sets the
source metadata fields before pushing the full bronze record to Zerobus.

Run this script from any machine with network access to Databricks and GitHub:

    pip install databricks-zerobus-ingest-sdk requests
    python push_zerobus.py
"""

import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q",
                       "databricks-zerobus-ingest-sdk", "requests"])

import json
import logging
import time
from datetime import datetime, timezone

import requests
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

from utils_exec import (
    BRONZE_FQN,
    CLIENT_ID,
    CLIENT_SECRET,
    GITHUB_EVENTS_URL,
    GITHUB_SOURCE,
    GITHUB_SOURCE_TYPE,
    WORKSPACE_URL,
    ZEROBUS_ENDPOINT,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

POLL_INTERVAL_SEC = 60
BATCH_SIZE = 100
RUN_DURATION_SEC = 5 * 60   # stop after 5 minutes (set to None to run indefinitely)


def _epoch_micros(iso_str: str) -> int:
    """Convert an ISO-8601 timestamp string to microseconds since epoch."""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1_000_000)


def _epoch_days(iso_str: str) -> int:
    """Convert an ISO-8601 timestamp string to days since epoch (DATE type)."""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (dt - epoch).days


def _now_micros() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1_000_000)


def fetch_github_events(etag: str | None = None) -> tuple[list[dict], str | None]:
    """Fetch the latest page of public GitHub events, respecting ETags."""
    headers = {"Accept": "application/vnd.github+json"}
    if etag:
        headers["If-None-Match"] = etag

    resp = requests.get(GITHUB_EVENTS_URL, headers=headers, timeout=30)

    if resp.status_code == 304:
        log.info("No new events (304 Not Modified)")
        return [], etag

    resp.raise_for_status()
    new_etag = resp.headers.get("ETag")
    events = resp.json()
    log.info("Fetched %d events from GitHub", len(events))
    return events, new_etag


def build_bronze_record(event: dict) -> dict:
    """Shape a raw GitHub event into the bronze table schema."""
    created_at = event.get("created_at", datetime.now(timezone.utc).isoformat())
    return {
        "data": json.dumps(event),
        "event_time": _epoch_micros(created_at),
        "event_date": _epoch_days(created_at),
        "source": GITHUB_SOURCE,
        "source_type": GITHUB_SOURCE_TYPE,
        "processed_time": _now_micros(),
    }


def main() -> None:
    log.info("Initializing Zerobus SDK  →  %s", BRONZE_FQN)

    sdk = ZerobusSdk(ZEROBUS_ENDPOINT, WORKSPACE_URL)
    table_props = TableProperties(BRONZE_FQN)
    options = StreamConfigurationOptions(record_type=RecordType.JSON)

    stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_props, options)
    log.info("Zerobus stream created")

    seen_ids: set[str] = set()
    etag: str | None = None
    start_time = time.monotonic()
    total_pushed = 0

    try:
        while True:
            if RUN_DURATION_SEC is not None:
                elapsed = time.monotonic() - start_time
                if elapsed >= RUN_DURATION_SEC:
                    log.info(
                        "Run duration reached (%ds). Pushed %d events total.",
                        RUN_DURATION_SEC,
                        total_pushed,
                    )
                    break

            events, etag = fetch_github_events(etag)

            new_events = [e for e in events if e.get("id") not in seen_ids]
            if not new_events:
                log.info("Sleeping %ds …", POLL_INTERVAL_SEC)
                time.sleep(POLL_INTERVAL_SEC)
                continue

            records = [build_bronze_record(e) for e in new_events]

            for batch_start in range(0, len(records), BATCH_SIZE):
                batch = records[batch_start : batch_start + BATCH_SIZE]
                stream.ingest_records_nowait(batch)

            stream.flush()
            seen_ids.update(e["id"] for e in new_events)
            total_pushed += len(new_events)
            log.info("Pushed %d new events (%d total)", len(new_events), total_pushed)

            time.sleep(POLL_INTERVAL_SEC)

    except KeyboardInterrupt:
        log.info("Shutting down …")
    finally:
        stream.close()
        log.info("Zerobus stream closed. %d events pushed.", total_pushed)


if __name__ == "__main__":
    main()
