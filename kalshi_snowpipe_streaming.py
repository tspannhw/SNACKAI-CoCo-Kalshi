#!/usr/bin/env python3
"""
kalshi_snowpipe_streaming.py — Stream Kalshi API data directly to Snowflake
Iceberg tables via Snowpipe Streaming v2 (High-Performance Architecture).

No Kafka required. Uses the snowpipe-streaming Python SDK to open channels
against each Iceberg table and append rows directly.

Usage:
    python3 kalshi_snowpipe_streaming.py                     # all endpoints, one-shot
    python3 kalshi_snowpipe_streaming.py --continuous 60      # loop every 60s
    python3 kalshi_snowpipe_streaming.py trades markets       # specific endpoints
    python3 kalshi_snowpipe_streaming.py --key-path keys/rsa_key.p8  # custom key

Environment Variables:
    SNOWFLAKE_ACCOUNT          Account identifier (default: SFSENORTHAMERICA-TSPANN_AWS1)
    SNOWFLAKE_USER             User for auth (default: KAFKAGUY)
    SNOWFLAKE_PRIVATE_KEY_PATH Path to RSA private key .p8 file
    SNOWFLAKE_ROLE             Role to use (default: ACCOUNTADMIN)
    SNOWFLAKE_DATABASE         Target database (default: KALSHI_DB)
    SNOWFLAKE_SCHEMA           Target schema (default: STREAMING)
    SNOWFLAKE_WAREHOUSE        Warehouse (default: INGEST)
"""

import asyncio
import json
import os
import signal
import sys
import tempfile
import time
from datetime import datetime, timezone

import aiohttp

try:
    from snowflake.ingest.streaming import StreamingIngestClient
except ImportError:
    print("ERROR: snowpipe-streaming not installed. Run: pip install snowpipe-streaming")
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

# Map endpoint names to Iceberg table names
TABLE_MAP = {
    "series":  "KALSHI_SERIES",
    "markets": "KALSHI_MARKETS",
    "trades":  "KALSHI_TRADES",
    "tags":    "KALSHI_CATEGORIES_TAGS",
    "sports":  "KALSHI_SPORTS_FILTERS",
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
# Snowpipe Streaming Client
# ---------------------------------------------------------------------------
def get_config():
    """Load Snowflake config from environment variables."""
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "")
    if not key_path:
        print("ERROR: SNOWFLAKE_PRIVATE_KEY_PATH must be set to the path of your RSA .p8 key file")
        sys.exit(1)

    if not os.path.isfile(key_path):
        print(f"ERROR: Private key file not found: {key_path}")
        sys.exit(1)

    return {
        "account": os.environ.get("SNOWFLAKE_ACCOUNT", "SFSENORTHAMERICA-TSPANN_AWS1"),
        "user": os.environ.get("SNOWFLAKE_USER", "KAFKAGUY"),
        "private_key_path": key_path,
        "role": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "database": os.environ.get("SNOWFLAKE_DATABASE", "KALSHI_DB"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "STREAMING"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "INGEST"),
    }


def create_streaming_client(config, table_name):
    """Create a StreamingIngestClient for a specific table."""
    with open(config["private_key_path"], "r") as f:
        private_key = f.read()

    profile = {
        "account": config["account"],
        "user": config["user"],
        "url": f"https://{config['account']}.snowflakecomputing.com:443",
        "private_key": private_key,
        "role": config["role"],
    }

    # Write profile to a secure temp file
    fd, profile_path = tempfile.mkstemp(suffix=".json", prefix="sf_profile_")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(profile, f)

        pipe_name = f"{table_name}-STREAMING"
        client = StreamingIngestClient(
            client_name=f"kalshi_{table_name.lower()}",
            db_name=config["database"],
            schema_name=config["schema"],
            pipe_name=pipe_name,
            profile_json=profile_path,
        )
    finally:
        # Remove the profile file immediately after client initialization
        os.unlink(profile_path)

    return client


# ---------------------------------------------------------------------------
# Async Fetchers (same as kalshi_kafka_producer.py)
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
# Row mappers — convert API records to Iceberg-compatible row dicts
# ---------------------------------------------------------------------------
def map_series_row(record):
    """Map a series API record to KALSHI_SERIES columns."""
    return {
        "ticker": record.get("ticker", ""),
        "title": record.get("title", ""),
        "category": record.get("category"),
        "frequency": record.get("frequency"),
        "tags": json.dumps(record.get("tags")) if record.get("tags") else None,
        "fee_type": record.get("fee_type"),
        "fee_multiplier": record.get("fee_multiplier"),
        "contract_url": record.get("contract_url"),
        "settlement_sources": json.dumps(record.get("settlement_sources")) if record.get("settlement_sources") else None,
        "last_updated_ts": record.get("last_updated_ts"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "record_metadata": json.dumps(record, default=str),
    }


def map_market_row(record):
    """Map a market API record to KALSHI_MARKETS columns."""
    return {
        "ticker": record.get("ticker", ""),
        "event_ticker": record.get("event_ticker"),
        "title": record.get("title"),
        "status": record.get("status"),
        "market_type": record.get("market_type"),
        "close_time": record.get("close_time"),
        "expiration_time": record.get("expiration_time"),
        "expected_expiration_time": record.get("expected_expiration_time"),
        "open_time": record.get("open_time"),
        "last_price_dollars": _to_float(record.get("last_price_dollars")),
        "yes_bid_dollars": _to_float(record.get("yes_bid_dollars")),
        "yes_ask_dollars": _to_float(record.get("yes_ask_dollars")),
        "no_bid_dollars": _to_float(record.get("no_bid_dollars")),
        "no_ask_dollars": _to_float(record.get("no_ask_dollars")),
        "volume_fp": _to_float(record.get("volume_fp")),
        "volume_24h_fp": _to_float(record.get("volume_24h_fp")),
        "open_interest_fp": _to_float(record.get("open_interest_fp")),
        "liquidity_dollars": _to_float(record.get("liquidity_dollars")),
        "result": record.get("result"),
        "notional_value_dollars": _to_float(record.get("notional_value_dollars")),
        "yes_sub_title": record.get("yes_sub_title"),
        "no_sub_title": record.get("no_sub_title"),
        "can_close_early": record.get("can_close_early"),
        "expiration_value": record.get("expiration_value"),
        "strike_type": record.get("strike_type"),
        "rules_primary": record.get("rules_primary"),
        "created_time": record.get("created_time"),
        "updated_time": record.get("updated_time"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "record_metadata": json.dumps(record, default=str),
    }


def map_trade_row(record):
    """Map a trade API record to KALSHI_TRADES columns."""
    return {
        "trade_id": record.get("trade_id", ""),
        "ticker": record.get("ticker", ""),
        "taker_side": record.get("taker_side"),
        "yes_price_dollars": _to_float(record.get("yes_price_dollars")),
        "no_price_dollars": _to_float(record.get("no_price_dollars")),
        "count_fp": _to_float(record.get("count_fp")),
        "created_time": record.get("created_time", ""),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "record_metadata": json.dumps(record, default=str),
    }


def map_tag_row(category, tag):
    """Map a category/tag pair to KALSHI_CATEGORIES_TAGS columns."""
    return {
        "category": category,
        "tag": tag,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "record_metadata": json.dumps({"category": category, "tag": tag}),
    }


def map_sport_row(sport, scope, competition):
    """Map a sport filter to KALSHI_SPORTS_FILTERS columns."""
    return {
        "sport": sport,
        "scope": scope,
        "competition": competition,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "record_metadata": json.dumps({"sport": sport, "scope": scope, "competition": competition}),
    }


def _to_float(val):
    """Safely convert string or numeric value to float. API returns dollar strings like '0.4500'."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Streaming Orchestrator
# ---------------------------------------------------------------------------
async def stream_endpoint(session, config, name):
    """Fetch one endpoint and stream directly to Snowflake."""
    t0 = time.monotonic()
    table_name = TABLE_MAP[name]
    client = None
    channel = None

    try:
        # Create client and open channel
        client = create_streaming_client(config, table_name)
        channel, _ = client.open_channel(channel_name=f"kalshi_{name}_ch1")

        # Fetch data
        if name == "series":
            records = await fetch_all_series(session)
            rows = [map_series_row(r) for r in records]
        elif name == "markets":
            records = await fetch_all_markets(session)
            rows = [map_market_row(r) for r in records]
        elif name == "trades":
            records = await fetch_all_trades(session)
            rows = [map_trade_row(r) for r in records]
        elif name == "tags":
            raw = await fetch_tags_by_categories(session)
            rows = []
            for category, tags in raw.items():
                for tag in (tags or []):
                    rows.append(map_tag_row(category, tag))
        elif name == "sports":
            raw = await fetch_sports_filters(session)
            rows = []
            for sport, info in raw.items():
                for scope in info.get("scopes", []):
                    rows.append(map_sport_row(sport, scope, ""))
                for comp_name, comp_info in info.get("competitions", {}).items():
                    for scope in comp_info.get("scopes", []):
                        rows.append(map_sport_row(sport, scope, comp_name))
        else:
            raise ValueError(f"Unknown endpoint: {name}")

        # Stream rows in batches
        batch_size = 1000
        streamed = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            for idx, row in enumerate(batch):
                channel.append_row(row, offset_token=str(streamed + idx))
            streamed += len(batch)

        # Wait for flush
        channel.wait_for_flush(timeout_seconds=60)

        ms = int((time.monotonic() - t0) * 1000)
        return name, streamed, ms, None

    except Exception as e:
        ms = int((time.monotonic() - t0) * 1000)
        return name, 0, ms, str(e)

    finally:
        if channel:
            try:
                channel.close()
            except Exception:
                pass
        if client:
            try:
                client.close()
            except Exception:
                pass


async def run_stream(endpoints=None):
    """Fetch and stream all specified endpoints to Snowflake."""
    if endpoints is None:
        endpoints = list(ENDPOINTS.keys())

    config = get_config()

    print(f"[{_ts()}] Snowpipe Streaming v2 → Snowflake Iceberg (Direct)")
    print(f"[{_ts()}] Account: {config['account']}")
    print(f"[{_ts()}] User: {config['user']}")
    print(f"[{_ts()}] Database: {config['database']}.{config['schema']}")
    print(f"[{_ts()}] Tables: {', '.join(TABLE_MAP[e] for e in endpoints)}")
    print(f"[{_ts()}] Fetching from Kalshi API...")

    t0 = time.monotonic()
    async with aiohttp.ClientSession() as session:
        # Stream endpoints sequentially (each opens its own channel)
        results = []
        for ep in endpoints:
            if _shutdown:
                break
            result = await stream_endpoint(session, config, ep)
            results.append(result)

    total_ms = int((time.monotonic() - t0) * 1000)

    print(f"\n{'Endpoint':<12} {'Streamed':>10} {'Time (ms)':>10} {'Status':>10}")
    print("-" * 46)
    total = 0
    errors = 0
    for r in results:
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
    parser = argparse.ArgumentParser(description="Kalshi → Snowpipe Streaming v2 (Direct to Iceberg)")
    parser.add_argument("endpoints", nargs="*", help="Endpoints: series markets trades tags sports")
    parser.add_argument("--key-path", default=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", ""),
                        help="Path to RSA private key .p8 file")
    parser.add_argument("--continuous", type=int, metavar="SECS",
                        help="Run continuously with N second interval")
    args = parser.parse_args()

    # Set key path from CLI arg if provided
    if args.key_path:
        os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"] = args.key_path

    endpoints = args.endpoints or None
    if endpoints:
        invalid = [e for e in endpoints if e not in ENDPOINTS]
        if invalid:
            print(f"Unknown endpoints: {', '.join(invalid)}")
            print(f"Valid: {', '.join(ENDPOINTS.keys())}")
            sys.exit(1)

    if args.continuous:
        print(f"[{_ts()}] Continuous mode: streaming every {args.continuous}s")
        print(f"[{_ts()}] Press Ctrl+C to stop\n")
        iteration = 0
        while not _shutdown:
            iteration += 1
            print(f"\n{'='*50}")
            print(f"[{_ts()}] Iteration {iteration}")
            print(f"{'='*50}")
            asyncio.run(run_stream(endpoints))
            if not _shutdown:
                print(f"\n[{_ts()}] Sleeping {args.continuous}s...")
                for _ in range(args.continuous):
                    if _shutdown:
                        break
                    time.sleep(1)
        print(f"\n[{_ts()}] Streamer stopped after {iteration} iterations.")
    else:
        asyncio.run(run_stream(endpoints))


if __name__ == "__main__":
    main()
