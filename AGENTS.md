# Kalshi Trading Data Pipeline
Ingests prediction market data from the Kalshi public API into Snowflake PostgreSQL and streams it via Kafka or directly via Snowpipe Streaming v2 into Snowflake Managed Iceberg Tables.

## Architecture

```
                    ┌──────────────┐
                    │  Kalshi API  │
                    │  (Public)    │
                    └──────┬───────┘
                           │
          ┌────────────────┼────────────────┐
          │                │                │
┌─────────▼─────────┐  ┌──▼───────────┐  ┌─▼──────────────────┐
│ kalshi_ingest.py   │  │ kalshi_kafka_ │  │ kalshi_snowpipe_    │
│ (Direct to PG)     │  │ producer.py   │  │ streaming.py        │
└─────────┬──────────┘  └──┬───────────┘  └─┬──────────────────┘
          │                │                │
┌─────────▼──────────┐  ┌─▼────────────┐   │ (Snowpipe Streaming v2)
│ Snowflake          │  │ Kafka        │   │
│ PostgreSQL         │  │ (RedPanda)   │   │
│ 6 tables           │  └──┬───────────┘   │
└────────────────────┘     │               │
                     ┌─────▼──────────┐    │
                     │ Snowflake Kafka │    │
                     │ Connector (SSv2)│    │
                     └─────┬──────────┘    │
                           │               │
                     ┌─────▼───────────────▼──┐
                     │ Snowflake Managed       │
                     │ Iceberg Tables          │
                     │ (Snowflake Storage)     │
                     └────────────────────────┘
```

## Data Source
- **API:** https://docs.kalshi.com/welcome
- **Base URL:** https://api.elections.kalshi.com/trade-api/v2
- **Auth:** None required (public endpoints)

## Snowflake Connection
- **ACCOUNT:** LXB29530
- **REGION:** us-east-1
- **ROLE:** ACCOUNTADMIN
- **WAREHOUSE:** INGEST
- **DATABASE:** KALSHI_DB
- **SCHEMA:** STREAMING
- **STORAGE:** SNOWFLAKE_MANAGED (no external volume required)
- **KAFKA USER:** KAFKAGUY (RSA key-pair auth, service user)
- **CONNECTION NAME:** tspann1

## PostgreSQL Connection
- **PGHOST:** (see .env)
- **PGPORT:** 5432
- **PGDATABASE:** postgres
- **PGUSER:** snowflake_admin
- **PGSSLMODE:** require

## Tech Stack
- **Direct Ingest:** Python 3.11 + async aiohttp + psycopg2
- **Kafka Streaming:** confluent-kafka Python producer → Snowflake Kafka Connector
- **Direct Streaming:** snowpipe-streaming Python SDK (Snowpipe Streaming v2 High-Performance)
- **Kafka Broker:** RedPanda (Docker) or StreamNative / RedPanda on 192.168.1.172
- **Kafka Connector:** Snowflake Kafka Connector 2.4.0 (Snowpipe Streaming)
- **Storage:** Snowflake Managed Iceberg Tables (Snowflake Managed Storage, Apache Iceberg v2 format)
- **Database:** Snowflake PostgreSQL + Snowflake (Iceberg)
- **Orchestration:** Bash (manage.sh)
- **LANGUAGE:** Python + SQL + Bash

## API Endpoints

| Endpoint | API Path | PG Table | Iceberg Table | Kafka Topic |
|----------|----------|----------|---------------|-------------|
| series | /series | kalshi_series | KALSHI_SERIES | kalshi-series |
| markets | /markets | kalshi_markets | KALSHI_MARKETS | kalshi-markets |
| trades | /markets/trades | kalshi_trades | KALSHI_TRADES | kalshi-trades |
| tags | /search/tags_by_categories | kalshi_categories_tags | KALSHI_CATEGORIES_TAGS | kalshi-tags |
| sports | /search/filters_by_sport | kalshi_sports_filters | KALSHI_SPORTS_FILTERS | kalshi-sports |

## PostgreSQL Tables

| Table | Primary Key | Rows | Description |
|-------|------------|------|-------------|
| kalshi_series | ticker | ~9,400 | Series metadata, tags, settlement sources |
| kalshi_markets | ticker | ~50,000 | Market pricing, status, volume, open interest |
| kalshi_trades | trade_id | ~50,000 | Individual trades with yes/no prices and quantities |
| kalshi_categories_tags | (category, tag) | ~80 | Reference: categories and their tags |
| kalshi_sports_filters | (sport, scope, competition) | ~360 | Reference: sports filter metadata |
| kalshi_ingestion_log | log_id | varies | Ingestion run tracking with timing and error details |

## Snowflake Iceberg Tables

| Table | Storage | Catalog | Format |
|-------|---------|---------|--------|
| KALSHI_DB.STREAMING.KALSHI_SERIES | SNOWFLAKE_MANAGED | SNOWFLAKE | Iceberg v2 |
| KALSHI_DB.STREAMING.KALSHI_MARKETS | SNOWFLAKE_MANAGED | SNOWFLAKE | Iceberg v2 |
| KALSHI_DB.STREAMING.KALSHI_TRADES | SNOWFLAKE_MANAGED | SNOWFLAKE | Iceberg v2 |
| KALSHI_DB.STREAMING.KALSHI_CATEGORIES_TAGS | SNOWFLAKE_MANAGED | SNOWFLAKE | Iceberg v2 |
| KALSHI_DB.STREAMING.KALSHI_SPORTS_FILTERS | SNOWFLAKE_MANAGED | SNOWFLAKE | Iceberg v2 |

## Snowpipe Streaming v2 Pipes

| Pipe | Source Topic | Target Table |
|------|-------------|--------------|
| KALSHI_SERIES_PIPE | kalshi-series | KALSHI_SERIES |
| KALSHI_MARKETS_PIPE | kalshi-markets | KALSHI_MARKETS |
| KALSHI_TRADES_PIPE | kalshi-trades | KALSHI_TRADES |
| KALSHI_TAGS_PIPE | kalshi-tags | KALSHI_CATEGORIES_TAGS |
| KALSHI_SPORTS_PIPE | kalshi-sports | KALSHI_SPORTS_FILTERS |

## Folder Layout
```
kalshi/
├── AGENTS.md                          # This file
├── .env                               # PostgreSQL connection variables (not in git)
├── .gitignore                         # Excludes .env, backups, keys, etc.
├── manage.sh                          # Pipeline management CLI (PG + Kafka + Streaming + Iceberg)
├── kalshi_ingest.py                   # Direct async Python → PG ingest
├── kalshi_kafka_producer.py           # Async Python → Kafka producer
├── kalshi_snowpipe_streaming.py       # Direct Snowpipe Streaming v2 → Iceberg (no Kafka)
├── sql/
│   ├── 001_pg_kalshi_tables.sql       # PG DDL: tables, indexes, comments
│   ├── 002_pg_kalshi_tests.sql        # PG test suite (13 tests)
│   ├── 010_sf_kalshi_iceberg_tables.sql   # Snowflake Iceberg table DDL
│   ├── 011_sf_kalshi_streaming_pipes.sql  # Snowpipe Streaming v2 pipes (for Kafka)
│   ├── 012_sf_kalshi_iceberg_tests.sql    # Iceberg test suite (10 tests)
│   ├── 013_sf_kalshi_streaming_direct_tests.sql  # Direct streaming test suite (7 tests)
│   ├── 014_sf_kalshi_governance.sql       # Data governance: tags, masking policies
│   └── 015_sf_kalshi_semantic_view.sql    # Semantic view for Cortex Analyst
├── kafka/
│   ├── docker-compose.yml             # RedPanda + Kafka Connect
│   ├── SF_connect.properties          # Snowflake connector config (reference)
│   ├── setup_kafka.sh                 # Kafka setup/deploy/status/stop script
│   └── snowflake_kafkaguy_rsa_key.p8  # RSA private key for KAFKAGUY (not in git)
└── backups/                           # pg_dump backup files (not in git)
```

## manage.sh Commands

### PostgreSQL Pipeline
```bash
./manage.sh install               # Create all PG tables and indexes
./manage.sh ingest                # Ingest all endpoints → PG
./manage.sh ingest trades markets # Ingest specific endpoints
./manage.sh start                 # Resume pipeline
./manage.sh stop                  # Stop pipeline
./manage.sh backup                # pg_dump all kalshi_* tables
```

### Kafka Streaming
```bash
./manage.sh kafka-setup           # Start RedPanda + deploy Snowflake connector
./manage.sh kafka-produce         # One-shot: all endpoints → Kafka
./manage.sh kafka-produce trades  # One-shot: specific endpoint
./manage.sh kafka-produce --continuous 60  # Continuous: every 60s
./manage.sh kafka-status          # Connector status + topic offsets
./manage.sh kafka-stop            # Stop all Kafka services
```

### Direct Streaming (no Kafka)
```bash
./manage.sh stream                # One-shot: all endpoints → Iceberg via Snowpipe Streaming v2
./manage.sh stream trades markets # One-shot: specific endpoints
./manage.sh stream --continuous 60 # Continuous: every 60s
./manage.sh stream-status         # Row counts and data freshness
./manage.sh stream-test           # Run direct streaming SQL test suite
```

### Snowflake Iceberg
```bash
./manage.sh iceberg-install       # Create Iceberg tables + streaming pipes
./manage.sh iceberg-status        # Row counts and data freshness
./manage.sh iceberg-validate      # Validate tables, pipes, managed storage
./manage.sh iceberg-test          # Run full Iceberg SQL test suite
```

### Monitoring & Diagnostics
```bash
./manage.sh status                # PG row counts + recent ingestion runs
./manage.sh health                # PG connectivity, API health, data freshness
./manage.sh validate              # PG deep validation
./manage.sh test                  # PG SQL test suite
./manage.sh logs                  # Ingestion log history
```

### Data Queries
```bash
./manage.sh markets               # Top 20 active markets by 24h volume
./manage.sh trades                # Most recent 20 trades
./manage.sh series                # All series grouped by category
```

### Maintenance
```bash
./manage.sh clean [N]             # Remove ingestion logs older than N days (default: 30)
./manage.sh drop                  # Drop all Kalshi PG tables (interactive confirm)
```

## Quick Start

### Path 1: Direct PG Ingest
```bash
source .env
./manage.sh install
./manage.sh ingest
./manage.sh validate
```

### Path 2: Kafka → Iceberg Streaming
```bash
# 1. Create Snowflake Iceberg tables
./manage.sh iceberg-install

# 2. Start Kafka infrastructure
./manage.sh kafka-setup

# 3. Stream data
./manage.sh kafka-produce

# 4. Verify data landed in Iceberg
./manage.sh iceberg-status
./manage.sh iceberg-test
```

### Path 3: Continuous Streaming
```bash
./manage.sh kafka-produce --continuous 60
# Data flows: Kalshi API → Kafka → Snowflake Iceberg every 60 seconds
```

### Path 4: Direct Streaming (no Kafka)
```bash
# Requires: pip install snowpipe-streaming aiohttp
# Requires: SNOWFLAKE_PRIVATE_KEY_PATH pointing to KAFKAGUY .p8 key

# 1. Create Iceberg tables (if not already done)
./manage.sh iceberg-install

# 2. Stream data directly to Iceberg
export SNOWFLAKE_PRIVATE_KEY_PATH=keys/rsa_key.p8
./manage.sh stream

# 3. Verify
./manage.sh stream-status
./manage.sh stream-test

# Continuous mode:
./manage.sh stream --continuous 60
# Data flows: Kalshi API → Snowpipe Streaming v2 → Iceberg every 60 seconds
```

## Kafka Broker Endpoints

| Broker | Address | Use Case |
|--------|---------|----------|
| Local Docker (RedPanda) | localhost:9092 | Development / testing |
| LAN RedPanda | 192.168.1.172:19092 | On-prem RedPanda |
| StreamNative | (configure in .env) | Managed Kafka service |
| Docker internal | redpanda:29092 | Container-to-container |

## Kafka Connector Config

The Snowflake Kafka Connector uses:
- **Connector:** `com.snowflake.kafka.connector.SnowflakeSinkConnector` v2.4.0
- **Ingestion:** `SNOWPIPE_STREAMING` (Snowpipe Streaming v2)
- **Auth:** RSA key-pair via KAFKAGUY service user
- **Format:** `SnowflakeJsonConverter` (JSON → Iceberg columns)
- **Buffer:** 10,000 records or 120s flush interval

## Ingestion Architecture (Direct PG)

1. **Concurrent fetching:** All 5 API endpoints fetched in parallel via `asyncio.gather`
2. **Cursor pagination:** Markets and trades auto-page (1,000/page, up to 50 pages)
3. **Bulk upserts:** `psycopg2.extras.execute_values` with `ON CONFLICT`
4. **Ingestion logging:** Tracked in `kalshi_ingestion_log` with timing
5. **Performance:** ~110K records in ~60 seconds

## Streaming Architecture (Kafka → Iceberg)

1. **Fetch:** Same async aiohttp fetchers as direct ingest
2. **Produce:** `confluent-kafka` Python producer with LZ4 compression, batched
3. **Transport:** 5 Kafka topics, 3 partitions each
4. **Sink:** Snowflake Kafka Connector → Snowpipe Streaming v2
5. **Storage:** Snowflake Managed Iceberg Tables (Snowflake Managed Storage, Iceberg v2)
6. **Continuous mode:** Configurable interval (--continuous N seconds)

## Direct Streaming Architecture (Python → Iceberg, no Kafka)

1. **Fetch:** Same async aiohttp fetchers as direct ingest
2. **Stream:** `snowpipe-streaming` Python SDK (High-Performance Architecture)
3. **Auth:** KAFKAGUY service user with RSA key-pair (PKCS8 .p8 file)
4. **Channels:** One channel per table, opened against auto-created pipes (`<TABLE>-STREAMING`)
5. **Batching:** 1,000 rows per batch with offset tracking
6. **Storage:** Snowflake Managed Iceberg Tables (Snowflake Managed Storage, Iceberg v2)
7. **Security:** Private key loaded from `SNOWFLAKE_PRIVATE_KEY_PATH` env var; profile JSON written to temp file and immediately deleted
8. **Continuous mode:** Configurable interval (--continuous N seconds)

### Environment Variables (Direct Streaming)

| Variable | Default | Description |
|----------|---------|-------------|
| SNOWFLAKE_PRIVATE_KEY_PATH | (required) | Path to RSA .p8 private key file |
| SNOWFLAKE_ACCOUNT | SFSENORTHAMERICA-TSPANN_AWS1 | Snowflake account identifier |
| SNOWFLAKE_USER | KAFKAGUY | Service user |
| SNOWFLAKE_ROLE | ACCOUNTADMIN | Role for streaming |
| SNOWFLAKE_DATABASE | KALSHI_DB | Target database |
| SNOWFLAKE_SCHEMA | STREAMING | Target schema |
| SNOWFLAKE_WAREHOUSE | INGEST | Warehouse |

## Test Suites

### PG Tests (002_pg_kalshi_tests.sql) — 13 tests
| Test | What it checks |
|------|---------------|
| T1 | All 6 tables exist |
| T2-T4 | Column counts for series, markets, trades |
| T5 | Tables contain data |
| T6 | Primary key uniqueness |
| T7 | NOT NULL constraints |
| T8 | No negative prices |
| T9 | Trade price consistency (yes + no = $1.00) |
| T10 | Market status values are known |
| T11 | Ingestion log health |
| T12 | Index existence (>= 8) |
| T13 | Data freshness (24h) |

### Iceberg Tests (012_sf_kalshi_iceberg_tests.sql) — 10 tests
| Test | What it checks |
|------|---------------|
| T1 | Database and schema exist |
| T2 | All 5 Iceberg tables exist |
| T3-T5 | Column counts for series, markets, trades |
| T6 | Pipe existence (>= 5 pipes) |
| T7 | Snowflake Managed Storage confirmation |
| T8 | Row counts per table |
| T9 | Data freshness |
| T10 | Price validation |

### Direct Streaming Tests (013_sf_kalshi_streaming_direct_tests.sql) — 7 tests
| Test | What it checks |
|------|---------------|
| T1 | Database and schema exist |
| T2 | All 5 Iceberg tables exist |
| T3 | Row counts per table |
| T4 | Data freshness (ingested within last hour) |
| T5 | NOT NULL ticker validation |
| T6 | No negative prices |
| T7 | Snowflake Managed Storage confirmation |

## Rules
### Never
- GRANT OR REVOKE roles -- RBAC is managed by the platform team
- Skip diff review on SQL changes
- Commit .env files, RSA keys, or credentials
- Store secrets in connector config files checked into git
- Drop or modify masking policies without security officer approval

### Prefer
- Multiple tests for everything
- Always validate SQL
- Upsert (ON CONFLICT) over blind INSERT
- Async/parallel fetching for API calls
- Idempotent operations (safe to re-run)
- Snowpipe Streaming v2 over classic Snowpipe
- Iceberg tables for open format interoperability
- Tag-based masking over direct column masking (for scalability)
- IS_ROLE_IN_SESSION over CURRENT_ROLE (for role hierarchy support)

## Data Governance

### Masking Policies
| Policy | Data Type | Behavior |
|--------|-----------|----------|
| price_mask | DOUBLE | ACCOUNTADMIN/DATA_ADMIN: raw, ANALYST: rounded, others: NULL |
| volume_mask | DOUBLE | ACCOUNTADMIN/DATA_ADMIN/ANALYST: raw, others: 0 |
| ticker_mask | STRING | Authorized roles: raw, others: ***RESTRICTED*** |
| trade_id_mask | STRING | ACCOUNTADMIN/DATA_ADMIN: raw, others: SHA2 hash |

### Tags
| Tag | Applied To | Purpose |
|-----|-----------|---------|
| market_data | KALSHI_MARKETS, KALSHI_TRADES | Auto-masks all DOUBLE columns via price_mask |
| pii_data | KALSHI_TRADES.trade_id | Hashes trade IDs for non-admin roles |

### Governance Commands
```bash
./manage.sh governance-install    # Apply 014_sf_kalshi_governance.sql
./manage.sh semantic-install      # Apply 015_sf_kalshi_semantic_view.sql
```

## Semantic View

- **Object:** `KALSHI_DB.STREAMING.KALSHI_ANALYTICS`
- **Dimensions:** 12 (market ticker, status, type, category, trade side, etc.)
- **Metrics:** 11 (total volume, OI, avg price, trade count, etc.)
- **Relationships:** trades → markets → series
- **Verified Queries:** 5 (top markets, recent trades, volume by category, etc.)
- **Used by:** Cortex Analyst for natural language queries

## Reference Docs
- https://docs.kalshi.com/welcome
- https://docs.kalshi.com/api-reference/market/get-markets
- https://docs.snowflake.com/en/user-guide/snowflake-postgres
- https://docs.snowflake.com/en/user-guide/kafka-connector/index
- https://docs.snowflake.com/en/user-guide/kafka-connector-streaming
- https://docs.snowflake.com/en/user-guide/snowpipe-streaming/data-load-snowpipe-streaming-overview
- https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview
- https://docs.snowflake.com/en/user-guide/tables-iceberg
- https://docs.snowflake.com/en/user-guide/tables-iceberg-storage
- https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume
