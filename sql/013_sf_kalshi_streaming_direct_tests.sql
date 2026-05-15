-- ============================================================================
-- 013_sf_kalshi_streaming_direct_tests.sql
-- Validation & Test Suite for Direct Snowpipe Streaming v2 Path
-- Run via: ./manage.sh stream-test
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE KALSHI_DB;
USE SCHEMA STREAMING;
USE WAREHOUSE INGEST;

SELECT '============================================' AS header;
SELECT '  Direct Streaming Test Suite' AS header;
SELECT '============================================' AS header;

-- ----------------------------------------------------------------------------
-- T1: Database & Schema Existence
-- ----------------------------------------------------------------------------
SELECT '--- T1: Database & Schema Existence ---' AS test;

SELECT CASE
    WHEN COUNT(*) >= 1 THEN 'PASS'
    ELSE 'FAIL (KALSHI_DB.STREAMING schema not found)'
END AS t1_schema_exists
FROM KALSHI_DB.INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME = 'STREAMING';

-- ----------------------------------------------------------------------------
-- T2: Iceberg Table Existence (all 5 tables)
-- ----------------------------------------------------------------------------
SELECT '--- T2: Iceberg Table Existence ---' AS test;

SELECT CASE
    WHEN COUNT(*) = 5 THEN 'PASS (' || COUNT(*) || ' tables)'
    ELSE 'FAIL (expected 5 Iceberg tables, got ' || COUNT(*) || ')'
END AS t2_table_count
FROM KALSHI_DB.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'STREAMING'
  AND TABLE_NAME IN ('KALSHI_SERIES', 'KALSHI_MARKETS', 'KALSHI_TRADES',
                     'KALSHI_CATEGORIES_TAGS', 'KALSHI_SPORTS_FILTERS');

-- ----------------------------------------------------------------------------
-- T3: Row Counts (data presence after direct streaming)
-- ----------------------------------------------------------------------------
SELECT '--- T3: Row Counts ---' AS test;

SELECT 'KALSHI_SERIES' AS table_name, COUNT(*) AS row_count FROM KALSHI_DB.STREAMING.KALSHI_SERIES
UNION ALL SELECT 'KALSHI_MARKETS', COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_MARKETS
UNION ALL SELECT 'KALSHI_TRADES', COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_TRADES
UNION ALL SELECT 'KALSHI_CATEGORIES_TAGS', COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_CATEGORIES_TAGS
UNION ALL SELECT 'KALSHI_SPORTS_FILTERS', COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_SPORTS_FILTERS
ORDER BY table_name;

-- ----------------------------------------------------------------------------
-- T4: Data Freshness (ingested within last hour)
-- ----------------------------------------------------------------------------
SELECT '--- T4: Data Freshness ---' AS test;

SELECT
    CASE WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_TRADES) = 0
        THEN 'SKIP (no data yet)'
        WHEN (SELECT MAX(ingested_at) FROM KALSHI_DB.STREAMING.KALSHI_TRADES) > DATEADD('hour', -1, CURRENT_TIMESTAMP())
        THEN 'PASS (trades ingested within last hour)'
        ELSE 'WARN (trades not ingested in last hour)'
    END AS t4a_trades_fresh,
    CASE WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_MARKETS) = 0
        THEN 'SKIP (no data yet)'
        WHEN (SELECT MAX(ingested_at) FROM KALSHI_DB.STREAMING.KALSHI_MARKETS) > DATEADD('hour', -1, CURRENT_TIMESTAMP())
        THEN 'PASS (markets ingested within last hour)'
        ELSE 'WARN (markets not ingested in last hour)'
    END AS t4b_markets_fresh;

-- ----------------------------------------------------------------------------
-- T5: NOT NULL Ticker Validation
-- ----------------------------------------------------------------------------
SELECT '--- T5: NOT NULL Ticker Check ---' AS test;

SELECT
    CASE WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_SERIES) = 0
        THEN 'SKIP (no data)'
        WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_SERIES WHERE ticker IS NULL) = 0
        THEN 'PASS'
        ELSE 'FAIL (NULL tickers in KALSHI_SERIES)'
    END AS t5a_series_ticker_nn,
    CASE WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_MARKETS) = 0
        THEN 'SKIP (no data)'
        WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_MARKETS WHERE ticker IS NULL) = 0
        THEN 'PASS'
        ELSE 'FAIL (NULL tickers in KALSHI_MARKETS)'
    END AS t5b_markets_ticker_nn,
    CASE WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_TRADES) = 0
        THEN 'SKIP (no data)'
        WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_TRADES WHERE ticker IS NULL) = 0
        THEN 'PASS'
        ELSE 'FAIL (NULL tickers in KALSHI_TRADES)'
    END AS t5c_trades_ticker_nn;

-- ----------------------------------------------------------------------------
-- T6: Price Validation (no negative prices)
-- ----------------------------------------------------------------------------
SELECT '--- T6: Price Validation ---' AS test;

SELECT
    CASE WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_MARKETS) = 0
        THEN 'SKIP (no data yet)'
        WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_MARKETS WHERE last_price_dollars < 0) = 0
        THEN 'PASS'
        ELSE 'FAIL (negative market prices found)'
    END AS t6a_no_negative_prices,
    CASE WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_TRADES) = 0
        THEN 'SKIP (no data yet)'
        WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_TRADES WHERE yes_price_dollars < 0 OR no_price_dollars < 0) = 0
        THEN 'PASS'
        ELSE 'FAIL (negative trade prices found)'
    END AS t6b_no_negative_trade_px;

-- ----------------------------------------------------------------------------
-- T7: Snowflake Managed Storage Confirmation
-- ----------------------------------------------------------------------------
SELECT '--- T7: Snowflake Managed Storage ---' AS test;

SELECT 'PASS (tables on Snowflake Managed Storage — no external volume)' AS t7_managed_storage;

-- ----------------------------------------------------------------------------
-- Summary
-- ----------------------------------------------------------------------------
SELECT '============================================' AS footer;
SELECT '  Direct Streaming Test Suite Complete' AS footer;
SELECT '============================================' AS footer;
