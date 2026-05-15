-- ============================================================================
-- 011_sf_kalshi_streaming_pipes.sql
-- Snowpipe Streaming v2 Pipes for Kalshi Kafka Connector
-- Maps Kafka topics → Iceberg tables via Snowflake Kafka Connector
--
-- Iceberg-compatible: TIMESTAMP_LTZ casts, STRING for metadata
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE KALSHI_DB;
USE SCHEMA STREAMING;
USE WAREHOUSE INGEST;

-- ----------------------------------------------------------------------------
-- 1. Series Pipe — kalshi-series topic → KALSHI_SERIES table
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PIPE KALSHI_SERIES_PIPE AS
COPY INTO KALSHI_DB.STREAMING.KALSHI_SERIES (
    ticker, title, category, frequency, tags, fee_type,
    fee_multiplier, contract_url, settlement_sources,
    last_updated_ts, ingested_at, record_metadata
)
FROM (
    SELECT
        $1:ticker::STRING,
        $1:title::STRING,
        $1:category::STRING,
        $1:frequency::STRING,
        $1:tags::STRING,
        $1:fee_type::STRING,
        $1:fee_multiplier::INT,
        $1:contract_url::STRING,
        $1:settlement_sources::STRING,
        $1:last_updated_ts::TIMESTAMP_LTZ,
        CURRENT_TIMESTAMP()::TIMESTAMP_LTZ,
        $1::STRING
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
);

COMMENT ON PIPE KALSHI_SERIES_PIPE IS
    'Snowpipe Streaming v2 — Kalshi series metadata from Kafka topic kalshi-series';

-- ----------------------------------------------------------------------------
-- 2. Markets Pipe — kalshi-markets topic → KALSHI_MARKETS table
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PIPE KALSHI_MARKETS_PIPE AS
COPY INTO KALSHI_DB.STREAMING.KALSHI_MARKETS (
    ticker, event_ticker, title, status, market_type,
    close_time, expiration_time, expected_expiration_time, open_time,
    last_price_dollars, yes_bid_dollars, yes_ask_dollars,
    no_bid_dollars, no_ask_dollars, volume_fp, volume_24h_fp,
    open_interest_fp, liquidity_dollars, result, notional_value_dollars,
    yes_sub_title, no_sub_title, can_close_early, expiration_value,
    strike_type, rules_primary, created_time, updated_time,
    ingested_at, record_metadata
)
FROM (
    SELECT
        $1:ticker::STRING,
        $1:event_ticker::STRING,
        $1:title::STRING,
        $1:status::STRING,
        $1:market_type::STRING,
        $1:close_time::TIMESTAMP_LTZ,
        $1:expiration_time::TIMESTAMP_LTZ,
        $1:expected_expiration_time::TIMESTAMP_LTZ,
        $1:open_time::TIMESTAMP_LTZ,
        $1:last_price_dollars::DOUBLE,
        $1:yes_bid_dollars::DOUBLE,
        $1:yes_ask_dollars::DOUBLE,
        $1:no_bid_dollars::DOUBLE,
        $1:no_ask_dollars::DOUBLE,
        $1:volume_fp::DOUBLE,
        $1:volume_24h_fp::DOUBLE,
        $1:open_interest_fp::DOUBLE,
        $1:liquidity_dollars::DOUBLE,
        $1:result::STRING,
        $1:notional_value_dollars::DOUBLE,
        $1:yes_sub_title::STRING,
        $1:no_sub_title::STRING,
        $1:can_close_early::BOOLEAN,
        $1:expiration_value::STRING,
        $1:strike_type::STRING,
        $1:rules_primary::STRING,
        $1:created_time::TIMESTAMP_LTZ,
        $1:updated_time::TIMESTAMP_LTZ,
        CURRENT_TIMESTAMP()::TIMESTAMP_LTZ,
        $1::STRING
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
);

COMMENT ON PIPE KALSHI_MARKETS_PIPE IS
    'Snowpipe Streaming v2 — Kalshi market listings from Kafka topic kalshi-markets';

-- ----------------------------------------------------------------------------
-- 3. Trades Pipe — kalshi-trades topic → KALSHI_TRADES table
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PIPE KALSHI_TRADES_PIPE AS
COPY INTO KALSHI_DB.STREAMING.KALSHI_TRADES (
    trade_id, ticker, taker_side, yes_price_dollars,
    no_price_dollars, count_fp, created_time,
    ingested_at, record_metadata
)
FROM (
    SELECT
        $1:trade_id::STRING,
        $1:ticker::STRING,
        $1:taker_side::STRING,
        $1:yes_price_dollars::DOUBLE,
        $1:no_price_dollars::DOUBLE,
        $1:count_fp::DOUBLE,
        $1:created_time::TIMESTAMP_LTZ,
        CURRENT_TIMESTAMP()::TIMESTAMP_LTZ,
        $1::STRING
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
);

COMMENT ON PIPE KALSHI_TRADES_PIPE IS
    'Snowpipe Streaming v2 — Kalshi trade executions from Kafka topic kalshi-trades';

-- ----------------------------------------------------------------------------
-- 4. Categories Tags Pipe — kalshi-tags topic → KALSHI_CATEGORIES_TAGS table
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PIPE KALSHI_TAGS_PIPE AS
COPY INTO KALSHI_DB.STREAMING.KALSHI_CATEGORIES_TAGS (
    category, tag, ingested_at, record_metadata
)
FROM (
    SELECT
        $1:category::STRING,
        $1:tag::STRING,
        CURRENT_TIMESTAMP()::TIMESTAMP_LTZ,
        $1::STRING
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
);

COMMENT ON PIPE KALSHI_TAGS_PIPE IS
    'Snowpipe Streaming v2 — Kalshi category tags from Kafka topic kalshi-tags';

-- ----------------------------------------------------------------------------
-- 5. Sports Filters Pipe — kalshi-sports topic → KALSHI_SPORTS_FILTERS table
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PIPE KALSHI_SPORTS_PIPE AS
COPY INTO KALSHI_DB.STREAMING.KALSHI_SPORTS_FILTERS (
    sport, scope, competition, ingested_at, record_metadata
)
FROM (
    SELECT
        $1:sport::STRING,
        $1:scope::STRING,
        $1:competition::STRING,
        CURRENT_TIMESTAMP()::TIMESTAMP_LTZ,
        $1::STRING
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
);

COMMENT ON PIPE KALSHI_SPORTS_PIPE IS
    'Snowpipe Streaming v2 — Kalshi sports filters from Kafka topic kalshi-sports';

-- ----------------------------------------------------------------------------
-- Verification
-- ----------------------------------------------------------------------------
SHOW PIPES IN SCHEMA KALSHI_DB.STREAMING;
