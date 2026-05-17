# Kalshi Trading Data Pipeline

Real-time prediction market data ingestion from the [Kalshi](https://kalshi.com) public API into Snowflake, with three ingestion paths: PostgreSQL direct, Kafka streaming, and Snowpipe Streaming v2 direct-to-Iceberg.

## Architecture

```
                    ┌──────────────┐
                    │  Kalshi API  │
                    │  (Public)    │
                    └──────┬───────┘
                           │
          ┌────────────────┼─────────────────┐
          │                │                 │
┌─────────▼──────────┐  ┌──▼────────────┐  ┌─▼──────────────────┐
│ kalshi_ingest.py   │  │ kalshi_kafka_ │  │ kalshi_snowpipe_   │
│ (Direct to PG)     │  │ producer.py   │  │ streaming.py       │
└─────────┬──────────┘  └──┬────────────┘  └─┬──────────────────┘
          │                │                 │
┌─────────▼──────────┐  ┌─▼────────────┐     │ (Snowpipe Streaming v2)
│ Snowflake          │  │ Kafka        │     │
│ PostgreSQL         │  │ (RedPanda)   │     │
│ 6 tables           │  └──┬───────────┘     │
└────────────────────┘     │                 │
                     ┌─────▼───────────┐     │
                     │ Snowflake Kafka │     │
                     │ Connector (SSv2)│     │
                     └─────┬───────────┘     │
                           │                 │
                     ┌─────▼─────────────────▼──┐
                     │ Snowflake Managed        │
                     │ Iceberg Tables           │
                     │ (Snowflake Storage)      │
                     └──────────────────────────┘
```

## Features

- **3 ingestion paths**: PostgreSQL direct, Kafka streaming (RedPanda), Snowpipe Streaming v2 (no Kafka)
- **Snowflake Managed Iceberg Tables**: Apache Iceberg v2 format, Snowflake-managed storage
- **Async concurrent fetching**: All 5 API endpoints fetched in parallel
- **Cursor-based pagination**: Auto-pages markets and trades (up to 50K records)
- **Continuous mode**: Configurable streaming interval for all paths
- **Data governance**: Tag-based masking policies for pricing and trade data
- **Semantic view**: Cortex Analyst integration with verified queries
- **Full test suites**: PG (13 tests), Iceberg (10 tests), Direct Streaming (7 tests)
- **Production security**: All credentials via environment variables, comprehensive `.gitignore`

## Prerequisites

- Python 3.11+
- Snowflake account with ACCOUNTADMIN access
- [Snow CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) configured
- Docker + Docker Compose (for Kafka path only)
- RSA key-pair for KAFKAGUY service user

## Quick Start

### 1. Setup

```bash
git clone <this-repo>
cd kalshi

# Install Python dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your Snowflake credentials
source .env
```

### 2. Choose Your Path

#### Path A: PostgreSQL Direct Ingest
```bash
./manage.sh install    # Create PG tables
./manage.sh ingest     # Fetch all endpoints → PG
./manage.sh validate   # Run validation suite
```

#### Path B: Kafka → Iceberg Streaming
```bash
./manage.sh iceberg-install   # Create Snowflake Iceberg tables
./manage.sh kafka-setup       # Start RedPanda + deploy connector
./manage.sh kafka-produce     # Stream all endpoints → Kafka → Iceberg
./manage.sh iceberg-test      # Verify data
```

#### Path C: Direct Snowpipe Streaming (No Kafka)
```bash
./manage.sh iceberg-install   # Create Iceberg tables (if not done)
./manage.sh stream            # Stream directly → Iceberg
./manage.sh stream-test       # Verify data
```

#### Continuous Mode (any streaming path)
```bash
./manage.sh kafka-produce --continuous 60   # Kafka path: every 60s
./manage.sh stream --continuous 60          # Direct path: every 60s
```

## Data Endpoints

| Endpoint | Records | Description |
|----------|---------|-------------|
| series | ~9,400 | Market series metadata |
| markets | ~50,000 | Live market pricing and status |
| trades | ~50,000 | Individual trade executions |
| tags | ~80 | Category/tag mappings |
| sports | ~360 | Sports filter metadata |

## Security

- All credentials loaded from environment variables (never hardcoded)
- `.gitignore` excludes: `.env`, `*.p8`, `*.pem`, `*.key`, `keys/`, `backups/`, profile JSON files
- RSA key-pair authentication for Snowflake (no passwords stored)
- Tag-based masking policies for sensitive pricing data
- Profile JSON files written to temp and immediately deleted

## Commands Reference

Run `./manage.sh help` for the full command list. Key commands:

| Command | Description |
|---------|-------------|
| `ingest` | PG ingest (all or specific endpoints) |
| `stream` | Direct Snowpipe Streaming to Iceberg |
| `kafka-produce` | Kafka streaming path |
| `iceberg-install` | Create Iceberg tables + pipes |
| `validate` | PG deep validation |
| `iceberg-test` | Iceberg test suite |
| `stream-test` | Direct streaming test suite |
| `status` | PG row counts |
| `stream-status` | Iceberg row counts + freshness |

## Project Structure

```
kalshi/
├── manage.sh                     # CLI for all pipeline operations
├── kalshi_ingest.py              # PG direct ingest
├── kalshi_kafka_producer.py      # Kafka producer
├── kalshi_snowpipe_streaming.py  # Snowpipe Streaming v2 (direct)
├── requirements.txt              # Python dependencies
├── .env.example                  # Environment variable template
├── sql/                          # All DDL and test SQL
├── kafka/                        # Docker Compose + connector config
└── AGENTS.md                     # Full developer documentation
```

## Documentation

See [AGENTS.md](AGENTS.md) for comprehensive developer documentation including:
- Full architecture details for all 3 paths
- Complete manage.sh command reference
- Kafka broker configuration
- Environment variable reference
- Test suite documentation
- Data governance policies
- Semantic view specification

## License

MIT
