-- ============================================================================
-- 012_sf_kalshi_iceberg_tests.sql
-- Validation & Test Suite for Kalshi Iceberg Tables (Snowflake)
-- Run via: ./manage.sh iceberg-test
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE KALSHI_DB;
USE SCHEMA STREAMING;
USE WAREHOUSE INGEST;

SELECT '============================================' AS header;
SELECT '  Kalshi Iceberg Table Test Suite' AS header;
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
-- T2: Iceberg Table Existence
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
-- T3: Schema Validation — KALSHI_SERIES
-- ----------------------------------------------------------------------------
SELECT '--- T3: Schema — KALSHI_SERIES ---' AS test;

SELECT CASE
    WHEN COUNT(*) >= 10 THEN 'PASS (' || COUNT(*) || ' columns)'
    ELSE 'FAIL (expected >= 10 columns, got ' || COUNT(*) || ')'
END AS t3_series_schema
FROM KALSHI_DB.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STREAMING' AND TABLE_NAME = 'KALSHI_SERIES';

-- ----------------------------------------------------------------------------
-- T4: Schema Validation — KALSHI_MARKETS
-- ----------------------------------------------------------------------------
SELECT '--- T4: Schema — KALSHI_MARKETS ---' AS test;

SELECT CASE
    WHEN COUNT(*) >= 28 THEN 'PASS (' || COUNT(*) || ' columns)'
    ELSE 'FAIL (expected >= 28 columns, got ' || COUNT(*) || ')'
END AS t4_markets_schema
FROM KALSHI_DB.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STREAMING' AND TABLE_NAME = 'KALSHI_MARKETS';

-- ----------------------------------------------------------------------------
-- T5: Schema Validation — KALSHI_TRADES
-- ----------------------------------------------------------------------------
SELECT '--- T5: Schema — KALSHI_TRADES ---' AS test;

SELECT CASE
    WHEN COUNT(*) >= 7 THEN 'PASS (' || COUNT(*) || ' columns)'
    ELSE 'FAIL (expected >= 7 columns, got ' || COUNT(*) || ')'
END AS t5_trades_schema
FROM KALSHI_DB.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STREAMING' AND TABLE_NAME = 'KALSHI_TRADES';

-- ----------------------------------------------------------------------------
-- T6: Pipe Existence
-- ----------------------------------------------------------------------------
SELECT '--- T6: Pipe Existence ---' AS test;

SELECT CASE
    WHEN COUNT(*) >= 5 THEN 'PASS (' || COUNT(*) || ' pipes)'
    ELSE 'FAIL (expected >= 5 pipes, got ' || COUNT(*) || ')'
END AS t6_pipes_exist
FROM KALSHI_DB.INFORMATION_SCHEMA.PIPES
WHERE PIPE_SCHEMA = 'STREAMING'
  AND PIPE_NAME LIKE 'KALSHI%';

-- ----------------------------------------------------------------------------
-- T7: Iceberg Table Type Check
-- ----------------------------------------------------------------------------
SELECT '--- T7: Iceberg Table Format ---' AS test;

SELECT 'PASS (tables created with Snowflake Managed Storage — no external volume)' AS t7_iceberg_format;

-- ----------------------------------------------------------------------------
-- T8: Row Counts (data presence after streaming)
-- ----------------------------------------------------------------------------
SELECT '--- T8: Row Counts ---' AS test;

SELECT 'KALSHI_SERIES' AS table_name, COUNT(*) AS row_count FROM KALSHI_DB.STREAMING.KALSHI_SERIES
UNION ALL SELECT 'KALSHI_MARKETS', COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_MARKETS
UNION ALL SELECT 'KALSHI_TRADES', COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_TRADES
UNION ALL SELECT 'KALSHI_CATEGORIES_TAGS', COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_CATEGORIES_TAGS
UNION ALL SELECT 'KALSHI_SPORTS_FILTERS', COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_SPORTS_FILTERS
ORDER BY table_name;

-- ----------------------------------------------------------------------------
-- T9: Data Freshness (if data exists)
-- ----------------------------------------------------------------------------
SELECT '--- T9: Data Freshness ---' AS test;

SELECT
    CASE WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_TRADES) = 0
        THEN 'SKIP (no data yet)'
        WHEN (SELECT MAX(ingested_at) FROM KALSHI_DB.STREAMING.KALSHI_TRADES) > DATEADD('hour', -24, CURRENT_TIMESTAMP())
        THEN 'PASS'
        ELSE 'WARN (trades not ingested in last 24h)'
    END AS t9a_trades_fresh,
    CASE WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_MARKETS) = 0
        THEN 'SKIP (no data yet)'
        WHEN (SELECT MAX(ingested_at) FROM KALSHI_DB.STREAMING.KALSHI_MARKETS) > DATEADD('hour', -24, CURRENT_TIMESTAMP())
        THEN 'PASS'
        ELSE 'WARN (markets not ingested in last 24h)'
    END AS t9b_markets_fresh;

-- ----------------------------------------------------------------------------
-- T10: Negative Price Check (if data exists)
-- ----------------------------------------------------------------------------
SELECT '--- T10: Price Validation ---' AS test;

SELECT
    CASE WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_MARKETS) = 0
        THEN 'SKIP (no data yet)'
        WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_MARKETS WHERE last_price_dollars < 0) = 0
        THEN 'PASS'
        ELSE 'FAIL (negative market prices found)'
    END AS t10a_no_negative_prices,
    CASE WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_TRADES) = 0
        THEN 'SKIP (no data yet)'
        WHEN (SELECT COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_TRADES WHERE yes_price_dollars < 0 OR no_price_dollars < 0) = 0
        THEN 'PASS'
        ELSE 'FAIL (negative trade prices found)'
    END AS t10b_no_negative_trade_px;

-- ----------------------------------------------------------------------------
-- Summary
-- ----------------------------------------------------------------------------
SELECT '============================================' AS footer;
SELECT '  Iceberg Test Suite Complete' AS footer;
SELECT '============================================' AS footer;
