-- ============================================================================
-- 002_pg_kalshi_tests.sql
-- Validation & Test Suite for Kalshi Trading Data Pipeline
-- Run via: ./manage.sh test
-- ============================================================================

\echo '============================================'
\echo '  Kalshi Pipeline Test Suite'
\echo '============================================'

-- ----------------------------------------------------------------------------
-- T1: Table Existence
-- ----------------------------------------------------------------------------
\echo ''
\echo '--- T1: Table Existence ---'

SELECT CASE
    WHEN count(*) = 6 THEN 'PASS'
    ELSE 'FAIL (expected 6 tables, got ' || count(*) || ')'
END AS t1_table_existence
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename IN (
    'kalshi_series', 'kalshi_markets', 'kalshi_trades',
    'kalshi_categories_tags', 'kalshi_sports_filters', 'kalshi_ingestion_log'
  );

-- ----------------------------------------------------------------------------
-- T2: Schema Validation — kalshi_series
-- ----------------------------------------------------------------------------
\echo '--- T2: Schema — kalshi_series ---'

SELECT CASE
    WHEN count(*) >= 11 THEN 'PASS (' || count(*) || ' columns)'
    ELSE 'FAIL (expected >= 11 columns, got ' || count(*) || ')'
END AS t2_series_schema
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'kalshi_series';

-- ----------------------------------------------------------------------------
-- T3: Schema Validation — kalshi_markets
-- ----------------------------------------------------------------------------
\echo '--- T3: Schema — kalshi_markets ---'

SELECT CASE
    WHEN count(*) >= 28 THEN 'PASS (' || count(*) || ' columns)'
    ELSE 'FAIL (expected >= 28 columns, got ' || count(*) || ')'
END AS t3_markets_schema
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'kalshi_markets';

-- ----------------------------------------------------------------------------
-- T4: Schema Validation — kalshi_trades
-- ----------------------------------------------------------------------------
\echo '--- T4: Schema — kalshi_trades ---'

SELECT CASE
    WHEN count(*) >= 8 THEN 'PASS (' || count(*) || ' columns)'
    ELSE 'FAIL (expected >= 8 columns, got ' || count(*) || ')'
END AS t4_trades_schema
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'kalshi_trades';

-- ----------------------------------------------------------------------------
-- T5: Non-Empty Tables (requires at least one ingest)
-- ----------------------------------------------------------------------------
\echo '--- T5: Data Loaded ---'

SELECT
    CASE WHEN (SELECT count(*) FROM kalshi_series) > 0
        THEN 'PASS' ELSE 'FAIL' END AS t5a_series_has_data,
    CASE WHEN (SELECT count(*) FROM kalshi_markets) > 0
        THEN 'PASS' ELSE 'FAIL' END AS t5b_markets_has_data,
    CASE WHEN (SELECT count(*) FROM kalshi_trades) > 0
        THEN 'PASS' ELSE 'FAIL' END AS t5c_trades_has_data;

-- ----------------------------------------------------------------------------
-- T6: Primary Key Uniqueness
-- ----------------------------------------------------------------------------
\echo '--- T6: Primary Key Uniqueness ---'

SELECT
    CASE WHEN (SELECT count(*) FROM kalshi_series) =
              (SELECT count(DISTINCT ticker) FROM kalshi_series)
        THEN 'PASS' ELSE 'FAIL (duplicate series tickers)' END AS t6a_series_pk,
    CASE WHEN (SELECT count(*) FROM kalshi_markets) =
              (SELECT count(DISTINCT ticker) FROM kalshi_markets)
        THEN 'PASS' ELSE 'FAIL (duplicate market tickers)' END AS t6b_markets_pk,
    CASE WHEN (SELECT count(*) FROM kalshi_trades) =
              (SELECT count(DISTINCT trade_id) FROM kalshi_trades)
        THEN 'PASS' ELSE 'FAIL (duplicate trade_ids)' END AS t6c_trades_pk;

-- ----------------------------------------------------------------------------
-- T7: NOT NULL Constraints
-- ----------------------------------------------------------------------------
\echo '--- T7: NOT NULL Constraints ---'

SELECT
    CASE WHEN (SELECT count(*) FROM kalshi_series WHERE ticker IS NULL) = 0
        THEN 'PASS' ELSE 'FAIL' END AS t7a_series_ticker_nn,
    CASE WHEN (SELECT count(*) FROM kalshi_trades WHERE ticker IS NULL) = 0
        THEN 'PASS' ELSE 'FAIL' END AS t7b_trades_ticker_nn,
    CASE WHEN (SELECT count(*) FROM kalshi_trades WHERE created_time IS NULL) = 0
        THEN 'PASS' ELSE 'FAIL' END AS t7c_trades_created_nn;

-- ----------------------------------------------------------------------------
-- T8: Price Range Validation (prices should be 0.00-1.00 for binary markets)
-- ----------------------------------------------------------------------------
\echo '--- T8: Price Range Validation ---'

SELECT
    CASE WHEN (SELECT count(*) FROM kalshi_markets
               WHERE last_price_dollars::NUMERIC < 0) = 0
        THEN 'PASS' ELSE 'FAIL (negative prices found)' END AS t8a_no_negative_prices,
    CASE WHEN (SELECT count(*) FROM kalshi_trades
               WHERE yes_price_dollars::NUMERIC < 0
                  OR no_price_dollars::NUMERIC < 0) = 0
        THEN 'PASS' ELSE 'FAIL (negative trade prices)' END AS t8b_no_negative_trade_px;

-- ----------------------------------------------------------------------------
-- T9: Trade Price Consistency (yes + no should approx = notional)
-- ----------------------------------------------------------------------------
\echo '--- T9: Trade Price Consistency ---'

SELECT CASE
    WHEN (SELECT count(*) FROM kalshi_trades
          WHERE yes_price_dollars IS NOT NULL
            AND no_price_dollars IS NOT NULL
            AND ABS(yes_price_dollars + no_price_dollars - 1.0) > 0.01) = 0
    THEN 'PASS'
    ELSE 'WARN (' || (SELECT count(*) FROM kalshi_trades
          WHERE yes_price_dollars IS NOT NULL
            AND no_price_dollars IS NOT NULL
            AND ABS(yes_price_dollars + no_price_dollars - 1.0) > 0.01) || ' trades with yes+no != $1.00)'
END AS t9_price_consistency;

-- ----------------------------------------------------------------------------
-- T10: Market Status Values
-- ----------------------------------------------------------------------------
\echo '--- T10: Market Status Values ---'

SELECT CASE
    WHEN (SELECT count(DISTINCT status) FROM kalshi_markets
          WHERE status NOT IN ('active', 'closed', 'settled', 'finalized',
                               'halted', 'inactive', 'canceled', 'determined',
                               'unopened', 'initialized')) = 0
    THEN 'PASS'
    ELSE 'WARN (unknown statuses: ' ||
         (SELECT string_agg(DISTINCT status, ', ')
          FROM kalshi_markets
          WHERE status NOT IN ('active', 'closed', 'settled', 'finalized',
                               'halted', 'inactive', 'canceled', 'determined',
                               'unopened', 'initialized')) || ')'
END AS t10_valid_statuses;

-- ----------------------------------------------------------------------------
-- T11: Ingestion Log Health
-- ----------------------------------------------------------------------------
\echo '--- T11: Ingestion Log Health ---'

SELECT
    CASE WHEN (SELECT count(*) FROM kalshi_ingestion_log WHERE status = 'success') > 0
        THEN 'PASS' ELSE 'FAIL (no successful ingestions)' END AS t11a_has_success,
    CASE WHEN (SELECT count(*) FROM kalshi_ingestion_log WHERE status = 'running'
               AND started_at < NOW() - INTERVAL '1 hour') = 0
        THEN 'PASS' ELSE 'WARN (stale running jobs)' END AS t11b_no_stale_running;

-- ----------------------------------------------------------------------------
-- T12: Index Existence
-- ----------------------------------------------------------------------------
\echo '--- T12: Index Existence ---'

SELECT CASE
    WHEN count(*) >= 8 THEN 'PASS (' || count(*) || ' indexes)'
    ELSE 'FAIL (expected >= 8 indexes, got ' || count(*) || ')'
END AS t12_indexes_exist
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename LIKE 'kalshi%';

-- ----------------------------------------------------------------------------
-- T13: Data Freshness
-- ----------------------------------------------------------------------------
\echo '--- T13: Data Freshness ---'

SELECT
    CASE WHEN (SELECT max(ingested_at) FROM kalshi_trades) > NOW() - INTERVAL '24 hours'
        THEN 'PASS' ELSE 'WARN (trades not ingested in last 24h)' END AS t13a_trades_fresh,
    CASE WHEN (SELECT max(ingested_at) FROM kalshi_markets) > NOW() - INTERVAL '24 hours'
        THEN 'PASS' ELSE 'WARN (markets not ingested in last 24h)' END AS t13b_markets_fresh;

-- ----------------------------------------------------------------------------
-- Summary
-- ----------------------------------------------------------------------------
\echo ''
\echo '--- Row Counts ---'

SELECT 'kalshi_series' AS table_name, count(*) AS rows FROM kalshi_series
UNION ALL SELECT 'kalshi_markets', count(*) FROM kalshi_markets
UNION ALL SELECT 'kalshi_trades', count(*) FROM kalshi_trades
UNION ALL SELECT 'kalshi_categories_tags', count(*) FROM kalshi_categories_tags
UNION ALL SELECT 'kalshi_sports_filters', count(*) FROM kalshi_sports_filters
UNION ALL SELECT 'kalshi_ingestion_log', count(*) FROM kalshi_ingestion_log
ORDER BY table_name;

\echo ''
\echo '--- Market Status Distribution ---'

SELECT status, count(*) AS cnt
FROM kalshi_markets
GROUP BY status
ORDER BY cnt DESC;

\echo ''
\echo '--- Trade Volume by Taker Side ---'

SELECT taker_side, count(*) AS trade_count,
       sum(count_fp) AS total_contracts,
       avg(yes_price_dollars)::NUMERIC(6,4) AS avg_yes_price
FROM kalshi_trades
GROUP BY taker_side
ORDER BY trade_count DESC;

\echo ''
\echo '--- Series by Category ---'

SELECT COALESCE(category, '(uncategorized)') AS category, count(*) AS series_count
FROM kalshi_series
GROUP BY category
ORDER BY series_count DESC;

\echo ''
\echo '============================================'
\echo '  Test Suite Complete'
\echo '============================================'
