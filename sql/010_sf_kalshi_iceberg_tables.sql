-- ============================================================================
-- 010_sf_kalshi_iceberg_tables.sql
-- Snowflake Managed Iceberg Tables for Kalshi Trading Data
-- Storage: SNOWFLAKE_MANAGED (no external volume required)
--
-- Iceberg type constraints:
--   - No VARIANT, ARRAY, OBJECT → use STRING (JSON serialized)
--   - No TIMESTAMP_TZ → use TIMESTAMP_LTZ
--   - No DEFAULT on TIMESTAMP_LTZ → supply in pipe/insert
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ----------------------------------------------------------------------------
-- Database & Schema
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS KALSHI_DB;
CREATE SCHEMA IF NOT EXISTS KALSHI_DB.STREAMING;

USE DATABASE KALSHI_DB;
USE SCHEMA STREAMING;
USE WAREHOUSE INGEST;

-- ----------------------------------------------------------------------------
-- 1. Series — Snowflake Managed Iceberg Table
-- ----------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE KALSHI_SERIES (
    ticker          STRING NOT NULL,
    title           STRING NOT NULL,
    category        STRING,
    frequency       STRING,
    tags            STRING,
    fee_type        STRING,
    fee_multiplier  INT,
    contract_url    STRING,
    settlement_sources STRING,
    last_updated_ts TIMESTAMP_LTZ,
    ingested_at     TIMESTAMP_LTZ,
    record_metadata STRING
)
    CATALOG = 'SNOWFLAKE'
    COMMENT = 'Kalshi prediction market series metadata — streamed via Kafka';

-- ----------------------------------------------------------------------------
-- 2. Markets — Snowflake Managed Iceberg Table
-- ----------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE KALSHI_MARKETS (
    ticker              STRING NOT NULL,
    event_ticker        STRING,
    title               STRING,
    status              STRING,
    market_type         STRING,
    close_time          TIMESTAMP_LTZ,
    expiration_time     TIMESTAMP_LTZ,
    expected_expiration_time TIMESTAMP_LTZ,
    open_time           TIMESTAMP_LTZ,
    last_price_dollars  DOUBLE,
    yes_bid_dollars     DOUBLE,
    yes_ask_dollars     DOUBLE,
    no_bid_dollars      DOUBLE,
    no_ask_dollars      DOUBLE,
    volume_fp           DOUBLE,
    volume_24h_fp       DOUBLE,
    open_interest_fp    DOUBLE,
    liquidity_dollars   DOUBLE,
    result              STRING,
    notional_value_dollars DOUBLE,
    yes_sub_title       STRING,
    no_sub_title        STRING,
    can_close_early     BOOLEAN,
    expiration_value    STRING,
    strike_type         STRING,
    rules_primary       STRING,
    created_time        TIMESTAMP_LTZ,
    updated_time        TIMESTAMP_LTZ,
    ingested_at         TIMESTAMP_LTZ,
    record_metadata     STRING
)
    CATALOG = 'SNOWFLAKE'
    COMMENT = 'Kalshi prediction market listings with pricing — streamed via Kafka';

-- ----------------------------------------------------------------------------
-- 3. Trades — Snowflake Managed Iceberg Table
-- ----------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE KALSHI_TRADES (
    trade_id            STRING NOT NULL,
    ticker              STRING NOT NULL,
    taker_side          STRING,
    yes_price_dollars   DOUBLE,
    no_price_dollars    DOUBLE,
    count_fp            DOUBLE,
    created_time        TIMESTAMP_LTZ NOT NULL,
    ingested_at         TIMESTAMP_LTZ,
    record_metadata     STRING
)
    CATALOG = 'SNOWFLAKE'
    COMMENT = 'Kalshi trade execution records — streamed via Kafka';

-- ----------------------------------------------------------------------------
-- 4. Categories & Tags — Snowflake Managed Iceberg Table
-- ----------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE KALSHI_CATEGORIES_TAGS (
    category        STRING NOT NULL,
    tag             STRING NOT NULL,
    ingested_at     TIMESTAMP_LTZ,
    record_metadata STRING
)
    CATALOG = 'SNOWFLAKE'
    COMMENT = 'Kalshi market categories and associated tags — streamed via Kafka';

-- ----------------------------------------------------------------------------
-- 5. Sports Filters — Snowflake Managed Iceberg Table
-- ----------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE KALSHI_SPORTS_FILTERS (
    sport           STRING NOT NULL,
    scope           STRING NOT NULL,
    competition     STRING,
    ingested_at     TIMESTAMP_LTZ,
    record_metadata STRING
)
    CATALOG = 'SNOWFLAKE'
    COMMENT = 'Kalshi sports market filter metadata — streamed via Kafka';

-- ----------------------------------------------------------------------------
-- Grant KAFKAGUY access
-- ----------------------------------------------------------------------------
GRANT USAGE ON DATABASE KALSHI_DB TO USER KAFKAGUY;
GRANT USAGE ON SCHEMA KALSHI_DB.STREAMING TO USER KAFKAGUY;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA KALSHI_DB.STREAMING TO USER KAFKAGUY;
GRANT USAGE ON WAREHOUSE INGEST TO USER KAFKAGUY;

-- ----------------------------------------------------------------------------
-- Verification
-- ----------------------------------------------------------------------------
SHOW ICEBERG TABLES IN SCHEMA KALSHI_DB.STREAMING;
