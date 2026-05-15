#!/usr/bin/env python3
"""
kalshi_ingest.py — High-speed Kalshi API ingest into Snowflake PostgreSQL.

Fetches trades, markets, series, categories/tags, and sports filters
concurrently using aiohttp, then bulk-upserts into PostgreSQL via
psycopg2 execute_values.

Usage:
    source .env
    python3 kalshi_ingest.py                  # all endpoints
    python3 kalshi_ingest.py trades           # single endpoint
    python3 kalshi_ingest.py markets trades   # multiple endpoints
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone

import aiohttp
import psycopg2
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

ENDPOINTS = {
    "series":     f"{BASE_URL}/series",
    "markets":    f"{BASE_URL}/markets",
    "trades":     f"{BASE_URL}/markets/trades",
    "tags":       f"{BASE_URL}/search/tags_by_categories",
    "sports":     f"{BASE_URL}/search/filters_by_sport",
}

PAGE_LIMIT = 1000  # max per request
MAX_PAGES = 50     # safety cap per paginated endpoint
CONCURRENT_PAGES = 5  # concurrent page fetches per endpoint


def get_pg_conn():
    return psycopg2.connect(
        host=os.environ["PGHOST"],
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "postgres"),
        user=os.environ.get("PGUSER", "snowflake_admin"),
        password=os.environ["PGPASSWORD"],
        sslmode=os.environ.get("PGSSLMODE", "require"),
    )


# ---------------------------------------------------------------------------
# Async Fetchers
# ---------------------------------------------------------------------------
async def fetch_json(session, url, params=None):
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
        resp.raise_for_status()
        return await resp.json()


async def fetch_paginated(session, url, collection_key, limit=PAGE_LIMIT):
    """Fetch all pages from a cursor-paginated Kalshi endpoint."""
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
    """Fetch all series (single page, no pagination needed usually)."""
    data = await fetch_json(session, ENDPOINTS["series"])
    return data.get("series", [])


async def fetch_all_markets(session):
    """Fetch all markets with cursor pagination."""
    return await fetch_paginated(session, ENDPOINTS["markets"], "markets")


async def fetch_all_trades(session):
    """Fetch recent trades with cursor pagination."""
    return await fetch_paginated(session, ENDPOINTS["trades"], "trades")


async def fetch_tags_by_categories(session):
    """Fetch tags grouped by category."""
    data = await fetch_json(session, ENDPOINTS["tags"])
    return data.get("tags_by_categories", {})


async def fetch_sports_filters(session):
    """Fetch sports filter metadata."""
    data = await fetch_json(session, ENDPOINTS["sports"])
    return data.get("filters_by_sports", {})


# ---------------------------------------------------------------------------
# PostgreSQL Bulk Loaders
# ---------------------------------------------------------------------------
def log_start(conn, endpoint):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO kalshi_ingestion_log (endpoint, status)
               VALUES (%s, 'running') RETURNING log_id""",
            (endpoint,),
        )
        log_id = cur.fetchone()[0]
        conn.commit()
        return log_id


def log_finish(conn, log_id, fetched, upserted, status="success", error=None, duration_ms=None):
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE kalshi_ingestion_log
               SET records_fetched = %s, records_upserted = %s,
                   completed_at = NOW(), status = %s, error_message = %s,
                   duration_ms = %s
               WHERE log_id = %s""",
            (fetched, upserted, status, error, duration_ms, log_id),
        )
        conn.commit()


def upsert_series(conn, series_list):
    if not series_list:
        return 0
    rows = []
    for s in series_list:
        rows.append((
            s["ticker"],
            s.get("title", ""),
            s.get("category"),
            s.get("frequency"),
            s.get("tags", []),
            s.get("fee_type"),
            s.get("fee_multiplier", 1),
            s.get("contract_url"),
            json.dumps(s.get("settlement_sources", [])),
            s.get("last_updated_ts"),
        ))

    sql = """
        INSERT INTO kalshi_series (
            ticker, title, category, frequency, tags, fee_type,
            fee_multiplier, contract_url, settlement_sources, last_updated_ts
        ) VALUES %s
        ON CONFLICT (ticker) DO UPDATE SET
            title = EXCLUDED.title,
            category = EXCLUDED.category,
            frequency = EXCLUDED.frequency,
            tags = EXCLUDED.tags,
            fee_type = EXCLUDED.fee_type,
            fee_multiplier = EXCLUDED.fee_multiplier,
            contract_url = EXCLUDED.contract_url,
            settlement_sources = EXCLUDED.settlement_sources,
            last_updated_ts = EXCLUDED.last_updated_ts,
            ingested_at = NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
        count = cur.rowcount
        conn.commit()
        return count


def upsert_markets(conn, markets_list):
    if not markets_list:
        return 0
    rows = []
    for m in markets_list:
        rows.append((
            m["ticker"],
            m.get("event_ticker"),
            m.get("title"),
            m.get("status"),
            m.get("market_type"),
            m.get("close_time"),
            m.get("expiration_time"),
            m.get("expected_expiration_time"),
            m.get("open_time"),
            m.get("last_price_dollars"),
            m.get("yes_bid_dollars"),
            m.get("yes_ask_dollars"),
            m.get("no_bid_dollars"),
            m.get("no_ask_dollars"),
            m.get("volume_fp"),
            m.get("volume_24h_fp"),
            m.get("open_interest_fp"),
            m.get("liquidity_dollars"),
            m.get("result"),
            m.get("notional_value_dollars"),
            m.get("yes_sub_title"),
            m.get("no_sub_title"),
            m.get("can_close_early"),
            m.get("expiration_value"),
            m.get("strike_type"),
            m.get("rules_primary"),
            m.get("created_time"),
            m.get("updated_time"),
        ))

    sql = """
        INSERT INTO kalshi_markets (
            ticker, event_ticker, title, status, market_type,
            close_time, expiration_time, expected_expiration_time, open_time,
            last_price_dollars, yes_bid_dollars, yes_ask_dollars,
            no_bid_dollars, no_ask_dollars, volume_fp, volume_24h_fp,
            open_interest_fp, liquidity_dollars, result, notional_value_dollars,
            yes_sub_title, no_sub_title, can_close_early, expiration_value,
            strike_type, rules_primary, created_time, updated_time
        ) VALUES %s
        ON CONFLICT (ticker) DO UPDATE SET
            event_ticker = EXCLUDED.event_ticker,
            title = EXCLUDED.title,
            status = EXCLUDED.status,
            market_type = EXCLUDED.market_type,
            close_time = EXCLUDED.close_time,
            expiration_time = EXCLUDED.expiration_time,
            expected_expiration_time = EXCLUDED.expected_expiration_time,
            open_time = EXCLUDED.open_time,
            last_price_dollars = EXCLUDED.last_price_dollars,
            yes_bid_dollars = EXCLUDED.yes_bid_dollars,
            yes_ask_dollars = EXCLUDED.yes_ask_dollars,
            no_bid_dollars = EXCLUDED.no_bid_dollars,
            no_ask_dollars = EXCLUDED.no_ask_dollars,
            volume_fp = EXCLUDED.volume_fp,
            volume_24h_fp = EXCLUDED.volume_24h_fp,
            open_interest_fp = EXCLUDED.open_interest_fp,
            liquidity_dollars = EXCLUDED.liquidity_dollars,
            result = EXCLUDED.result,
            notional_value_dollars = EXCLUDED.notional_value_dollars,
            yes_sub_title = EXCLUDED.yes_sub_title,
            no_sub_title = EXCLUDED.no_sub_title,
            can_close_early = EXCLUDED.can_close_early,
            expiration_value = EXCLUDED.expiration_value,
            strike_type = EXCLUDED.strike_type,
            rules_primary = EXCLUDED.rules_primary,
            created_time = EXCLUDED.created_time,
            updated_time = EXCLUDED.updated_time,
            ingested_at = NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
        count = cur.rowcount
        conn.commit()
        return count


def upsert_trades(conn, trades_list):
    if not trades_list:
        return 0
    rows = []
    for t in trades_list:
        rows.append((
            t["trade_id"],
            t["ticker"],
            t.get("taker_side"),
            t.get("yes_price_dollars"),
            t.get("no_price_dollars"),
            t.get("count_fp"),
            t["created_time"],
        ))

    sql = """
        INSERT INTO kalshi_trades (
            trade_id, ticker, taker_side, yes_price_dollars,
            no_price_dollars, count_fp, created_time
        ) VALUES %s
        ON CONFLICT (trade_id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
        count = cur.rowcount
        conn.commit()
        return count


def upsert_categories_tags(conn, tags_dict):
    if not tags_dict:
        return 0
    rows = []
    for category, tags in tags_dict.items():
        for tag in (tags or []):
            rows.append((category, tag))

    sql = """
        INSERT INTO kalshi_categories_tags (category, tag)
        VALUES %s
        ON CONFLICT (category, tag) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
        count = cur.rowcount
        conn.commit()
        return count


def upsert_sports_filters(conn, sports_dict):
    if not sports_dict:
        return 0
    rows = []
    for sport, info in sports_dict.items():
        # Global scopes (no competition)
        for scope in info.get("scopes", []):
            rows.append((sport, scope, ""))
        # Per-competition scopes
        for comp_name, comp_info in info.get("competitions", {}).items():
            for scope in comp_info.get("scopes", []):
                rows.append((sport, scope, comp_name))

    sql = """
        INSERT INTO kalshi_sports_filters (sport, scope, competition)
        VALUES %s
        ON CONFLICT (sport, scope, competition) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
        count = cur.rowcount
        conn.commit()
        return count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
async def ingest_endpoint(session, conn, name):
    """Fetch from one endpoint and load into PG. Returns (name, fetched, upserted, ms)."""
    t0 = time.monotonic()
    log_id = log_start(conn, name)

    try:
        if name == "series":
            data = await fetch_all_series(session)
            upserted = upsert_series(conn, data)
        elif name == "markets":
            data = await fetch_all_markets(session)
            upserted = upsert_markets(conn, data)
        elif name == "trades":
            data = await fetch_all_trades(session)
            upserted = upsert_trades(conn, data)
        elif name == "tags":
            data = await fetch_tags_by_categories(session)
            fetched_count = sum(len(v) for v in data.values() if v)
            upserted = upsert_categories_tags(conn, data)
            duration_ms = int((time.monotonic() - t0) * 1000)
            log_finish(conn, log_id, fetched_count, upserted, duration_ms=duration_ms)
            return name, fetched_count, upserted, duration_ms
        elif name == "sports":
            data = await fetch_sports_filters(session)
            fetched_count = sum(
                len(info.get("scopes", []))
                + sum(len(c.get("scopes", [])) for c in info.get("competitions", {}).values())
                for info in data.values()
            )
            upserted = upsert_sports_filters(conn, data)
            duration_ms = int((time.monotonic() - t0) * 1000)
            log_finish(conn, log_id, fetched_count, upserted, duration_ms=duration_ms)
            return name, fetched_count, upserted, duration_ms
        else:
            raise ValueError(f"Unknown endpoint: {name}")

        fetched = len(data) if isinstance(data, list) else 0
        duration_ms = int((time.monotonic() - t0) * 1000)
        log_finish(conn, log_id, fetched, upserted, duration_ms=duration_ms)
        return name, fetched, upserted, duration_ms

    except Exception as e:
        duration_ms = int((time.monotonic() - t0) * 1000)
        log_finish(conn, log_id, 0, 0, status="error", error=str(e), duration_ms=duration_ms)
        return name, 0, 0, duration_ms


async def run_ingest(endpoints=None):
    """Main entry: fetch all endpoints concurrently, load into PG."""
    if endpoints is None:
        endpoints = list(ENDPOINTS.keys())

    conn = get_pg_conn()
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Connected to PostgreSQL")
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Ingesting: {', '.join(endpoints)}")

    t0 = time.monotonic()

    async with aiohttp.ClientSession() as session:
        tasks = [ingest_endpoint(session, conn, ep) for ep in endpoints]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    total_ms = int((time.monotonic() - t0) * 1000)

    print(f"\n{'Endpoint':<12} {'Fetched':>10} {'Upserted':>10} {'Time (ms)':>10}")
    print("-" * 46)
    total_fetched = 0
    total_upserted = 0
    for r in results:
        if isinstance(r, Exception):
            print(f"{'ERROR':<12} {str(r)}")
        else:
            name, fetched, upserted, ms = r
            total_fetched += fetched
            total_upserted += upserted
            print(f"{name:<12} {fetched:>10,} {upserted:>10,} {ms:>10,}")

    print("-" * 46)
    print(f"{'TOTAL':<12} {total_fetched:>10,} {total_upserted:>10,} {total_ms:>10,}")

    # Print row counts
    print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] PostgreSQL row counts:")
    with conn.cursor() as cur:
        for table in [
            "kalshi_series", "kalshi_markets", "kalshi_trades",
            "kalshi_categories_tags", "kalshi_sports_filters", "kalshi_ingestion_log",
        ]:
            cur.execute(f"SELECT count(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"  {table:<30} {count:>10,}")

    conn.close()
    print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Done.")


def main():
    endpoints = sys.argv[1:] if len(sys.argv) > 1 else None
    if endpoints:
        invalid = [e for e in endpoints if e not in ENDPOINTS]
        if invalid:
            print(f"Unknown endpoints: {', '.join(invalid)}")
            print(f"Valid: {', '.join(ENDPOINTS.keys())}")
            sys.exit(1)

    asyncio.run(run_ingest(endpoints))


if __name__ == "__main__":
    main()
