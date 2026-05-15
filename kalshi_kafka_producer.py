#!/usr/bin/env python3
"""
kalshi_kafka_producer.py — Stream Kalshi API data to Kafka topics.

Fetches trades, markets, series, categories/tags, and sports filters
concurrently using aiohttp, then produces JSON records to Kafka topics
for consumption by the Snowflake Kafka Connector.

Usage:
    python3 kalshi_kafka_producer.py                     # all endpoints, one-shot
    python3 kalshi_kafka_producer.py --continuous 60      # loop every 60s
    python3 kalshi_kafka_producer.py trades markets       # specific endpoints
    python3 kalshi_kafka_producer.py --broker 192.168.1.172:9092  # custom broker

Environment Variables:
    KAFKA_BROKER       Bootstrap server (default: localhost:9092)
    KAFKA_TOPIC_PREFIX Topic prefix (default: kalshi)
"""

import asyncio
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import aiohttp

try:
    from confluent_kafka import Producer
except ImportError:
    print("ERROR: confluent-kafka not installed. Run: pip install confluent-kafka")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

ENDPOINTS = {
    "series":  f"{BASE_URL}/series",
    "markets": f"{BASE_URL}/markets",
    "trades":  f"{BASE_URL}/markets/trades",
    "tags":    f"{BASE_URL}/search/tags_by_categories",
    "sports":  f"{BASE_URL}/search/filters_by_sport",
}

TOPIC_MAP = {
    "series":  "kalshi-series",
    "markets": "kalshi-markets",
    "trades":  "kalshi-trades",
    "tags":    "kalshi-tags",
    "sports":  "kalshi-sports",
}

PAGE_LIMIT = 1000
MAX_PAGES = 50

# Graceful shutdown
_shutdown = False

def _handle_signal(signum, frame):
    global _shutdown
    _shutdown = True
    print(f"\n[{_ts()}] Shutdown signal received, finishing current batch...")

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def _ts():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


# ---------------------------------------------------------------------------
# Kafka Producer
# ---------------------------------------------------------------------------
def create_producer(broker: str) -> Producer:
    conf = {
        "bootstrap.servers": broker,
        "client.id": "kalshi-producer",
        "linger.ms": 50,
        "batch.num.messages": 500,
        "compression.type": "lz4",
        "acks": "all",
        "retries": 3,
        "retry.backoff.ms": 500,
    }
    return Producer(conf)


def delivery_callback(err, msg):
    if err:
        print(f"  [ERROR] Delivery failed: {err}")


def produce_records(producer: Producer, topic: str, records: list, key_field: str = None):
    """Produce a list of dict records to a Kafka topic."""
    count = 0
    for record in records:
        key = record.get(key_field, "").encode("utf-8") if key_field else None
        value = json.dumps(record, default=str).encode("utf-8")
        producer.produce(topic, value=value, key=key, callback=delivery_callback)
        count += 1
        if count % 1000 == 0:
            producer.poll(0)
    producer.flush(timeout=30)
    return count


# ---------------------------------------------------------------------------
# Async Fetchers (reused from kalshi_ingest.py)
# ---------------------------------------------------------------------------
async def fetch_json(session, url, params=None):
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
        resp.raise_for_status()
        return await resp.json()


async def fetch_paginated(session, url, collection_key, limit=PAGE_LIMIT):
    all_items = []
    cursor = None
    page = 0
    while page < MAX_PAGES:
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        data = await fetch_json(session, url, params)
        items = data.get(collection_key, [])
        all_items.extend(items)
        page += 1
        cursor = data.get("cursor")
        if not cursor or len(items) < limit:
            break
    return all_items


async def fetch_all_series(session):
    data = await fetch_json(session, ENDPOINTS["series"])
    return data.get("series", [])


async def fetch_all_markets(session):
    return await fetch_paginated(session, ENDPOINTS["markets"], "markets")


async def fetch_all_trades(session):
    return await fetch_paginated(session, ENDPOINTS["trades"], "trades")


async def fetch_tags_by_categories(session):
    data = await fetch_json(session, ENDPOINTS["tags"])
    return data.get("tags_by_categories", {})


async def fetch_sports_filters(session):
    data = await fetch_json(session, ENDPOINTS["sports"])
    return data.get("filters_by_sports", {})


# ---------------------------------------------------------------------------
# Flatten helpers (tags/sports need flattening for row-per-record topics)
# ---------------------------------------------------------------------------
def flatten_tags(tags_dict: dict) -> list:
    rows = []
    for category, tags in tags_dict.items():
        for tag in (tags or []):
            rows.append({"category": category, "tag": tag})
    return rows


def flatten_sports(sports_dict: dict) -> list:
    rows = []
    for sport, info in sports_dict.items():
        for scope in info.get("scopes", []):
            rows.append({"sport": sport, "scope": scope, "competition": ""})
        for comp_name, comp_info in info.get("competitions", {}).items():
            for scope in comp_info.get("scopes", []):
                rows.append({"sport": sport, "scope": scope, "competition": comp_name})
    return rows


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
async def produce_endpoint(session, producer, name, topic_prefix):
    """Fetch one endpoint and produce to Kafka."""
    t0 = time.monotonic()
    topic = f"{topic_prefix}-{name}" if topic_prefix != "kalshi" else TOPIC_MAP[name]

    try:
        if name == "series":
            records = await fetch_all_series(session)
            count = produce_records(producer, topic, records, key_field="ticker")
        elif name == "markets":
            records = await fetch_all_markets(session)
            count = produce_records(producer, topic, records, key_field="ticker")
        elif name == "trades":
            records = await fetch_all_trades(session)
            count = produce_records(producer, topic, records, key_field="trade_id")
        elif name == "tags":
            raw = await fetch_tags_by_categories(session)
            records = flatten_tags(raw)
            count = produce_records(producer, topic, records)
        elif name == "sports":
            raw = await fetch_sports_filters(session)
            records = flatten_sports(raw)
            count = produce_records(producer, topic, records)
        else:
            raise ValueError(f"Unknown endpoint: {name}")

        ms = int((time.monotonic() - t0) * 1000)
        return name, count, ms, None

    except Exception as e:
        ms = int((time.monotonic() - t0) * 1000)
        return name, 0, ms, str(e)


async def run_produce(endpoints=None, broker="localhost:9092", topic_prefix="kalshi"):
    if endpoints is None:
        endpoints = list(ENDPOINTS.keys())

    producer = create_producer(broker)
    print(f"[{_ts()}] Kafka broker: {broker}")
    print(f"[{_ts()}] Topics: {', '.join(TOPIC_MAP[e] for e in endpoints)}")
    print(f"[{_ts()}] Fetching from Kalshi API...")

    t0 = time.monotonic()
    async with aiohttp.ClientSession() as session:
        tasks = [produce_endpoint(session, producer, ep, topic_prefix) for ep in endpoints]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    total_ms = int((time.monotonic() - t0) * 1000)

    print(f"\n{'Endpoint':<12} {'Produced':>10} {'Time (ms)':>10} {'Status':>10}")
    print("-" * 46)
    total = 0
    errors = 0
    for r in results:
        if isinstance(r, Exception):
            print(f"{'ERROR':<12} {str(r)}")
            errors += 1
        else:
            name, count, ms, err = r
            total += count
            status = "OK" if not err else "ERROR"
            if err:
                errors += 1
            print(f"{name:<12} {count:>10,} {ms:>10,} {status:>10}")
            if err:
                print(f"  -> {err}")

    print("-" * 46)
    print(f"{'TOTAL':<12} {total:>10,} {total_ms:>10,}")
    if errors:
        print(f"  {errors} endpoint(s) had errors")

    return total, errors


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Kalshi → Kafka Producer")
    parser.add_argument("endpoints", nargs="*", help="Endpoints: series markets trades tags sports")
    parser.add_argument("--broker", default=os.environ.get("KAFKA_BROKER", "localhost:9092"),
                        help="Kafka bootstrap server (default: localhost:9092)")
    parser.add_argument("--prefix", default=os.environ.get("KAFKA_TOPIC_PREFIX", "kalshi"),
                        help="Topic prefix (default: kalshi)")
    parser.add_argument("--continuous", type=int, metavar="SECS",
                        help="Run continuously with N second interval")
    args = parser.parse_args()

    endpoints = args.endpoints or None
    if endpoints:
        invalid = [e for e in endpoints if e not in ENDPOINTS]
        if invalid:
            print(f"Unknown endpoints: {', '.join(invalid)}")
            print(f"Valid: {', '.join(ENDPOINTS.keys())}")
            sys.exit(1)

    if args.continuous:
        print(f"[{_ts()}] Continuous mode: producing every {args.continuous}s")
        print(f"[{_ts()}] Press Ctrl+C to stop\n")
        iteration = 0
        while not _shutdown:
            iteration += 1
            print(f"\n{'='*50}")
            print(f"[{_ts()}] Iteration {iteration}")
            print(f"{'='*50}")
            asyncio.run(run_produce(endpoints, args.broker, args.prefix))
            if not _shutdown:
                print(f"\n[{_ts()}] Sleeping {args.continuous}s...")
                for _ in range(args.continuous):
                    if _shutdown:
                        break
                    time.sleep(1)
        print(f"\n[{_ts()}] Producer stopped after {iteration} iterations.")
    else:
        asyncio.run(run_produce(endpoints, args.broker, args.prefix))


if __name__ == "__main__":
    main()
