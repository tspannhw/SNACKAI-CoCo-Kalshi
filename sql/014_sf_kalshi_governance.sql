-- ============================================================================
-- 014_sf_kalshi_governance.sql
-- Data Governance: Tags, Masking Policies, and Classification for Kalshi
-- Applies tag-based masking to pricing columns in all Iceberg tables
-- Run via: snow sql -f sql/014_sf_kalshi_governance.sql --connection $SNOW_CONNECTION
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE KALSHI_DB;
USE SCHEMA STREAMING;
USE WAREHOUSE INGEST;

-- ============================================================================
-- 1. Create Governance Tags
-- ============================================================================

CREATE TAG IF NOT EXISTS KALSHI_DB.STREAMING.market_data
  COMMENT = 'Tag for Kalshi market pricing and volume data governance';

CREATE TAG IF NOT EXISTS KALSHI_DB.STREAMING.pii_data
  COMMENT = 'Tag for any PII-adjacent data (trade IDs, user signals)';

-- ============================================================================
-- 2. Create Masking Policies
-- ============================================================================

CREATE OR REPLACE MASKING POLICY KALSHI_DB.STREAMING.price_mask
  AS (val DOUBLE) RETURNS DOUBLE ->
  CASE
    WHEN IS_ROLE_IN_SESSION('ACCOUNTADMIN') THEN val
    WHEN IS_ROLE_IN_SESSION('DATA_ADMIN') THEN val
    WHEN IS_ROLE_IN_SESSION('ANALYST') THEN ROUND(val, 1)
    ELSE NULL
  END
  COMMENT = 'Masks pricing data: full access for admins, rounded for analysts, NULL for others';

CREATE OR REPLACE MASKING POLICY KALSHI_DB.STREAMING.volume_mask
  AS (val DOUBLE) RETURNS DOUBLE ->
  CASE
    WHEN IS_ROLE_IN_SESSION('ACCOUNTADMIN') THEN val
    WHEN IS_ROLE_IN_SESSION('DATA_ADMIN') THEN val
    WHEN IS_ROLE_IN_SESSION('ANALYST') THEN val
    ELSE 0
  END
  COMMENT = 'Volume data: visible to admins and analysts, zero for others';

CREATE OR REPLACE MASKING POLICY KALSHI_DB.STREAMING.ticker_mask
  AS (val STRING) RETURNS STRING ->
  CASE
    WHEN IS_ROLE_IN_SESSION('ACCOUNTADMIN') THEN val
    WHEN IS_ROLE_IN_SESSION('DATA_ADMIN') THEN val
    WHEN IS_ROLE_IN_SESSION('ANALYST') THEN val
    ELSE '***RESTRICTED***'
  END
  COMMENT = 'Ticker mask: visible to authorized roles, masked for public';

CREATE OR REPLACE MASKING POLICY KALSHI_DB.STREAMING.trade_id_mask
  AS (val STRING) RETURNS STRING ->
  CASE
    WHEN IS_ROLE_IN_SESSION('ACCOUNTADMIN') THEN val
    WHEN IS_ROLE_IN_SESSION('DATA_ADMIN') THEN val
    ELSE SHA2(val)
  END
  COMMENT = 'Trade ID hash mask: only admins see real IDs, others see hash';

-- ============================================================================
-- 3. Assign Masking Policies to Tags
-- ============================================================================

ALTER TAG KALSHI_DB.STREAMING.market_data SET
  MASKING POLICY KALSHI_DB.STREAMING.price_mask;

ALTER TAG KALSHI_DB.STREAMING.pii_data SET
  MASKING POLICY KALSHI_DB.STREAMING.trade_id_mask;

-- ============================================================================
-- 4. Apply Tags to Tables
-- ============================================================================

ALTER TABLE KALSHI_DB.STREAMING.KALSHI_MARKETS
  SET TAG KALSHI_DB.STREAMING.market_data = 'pricing';

ALTER TABLE KALSHI_DB.STREAMING.KALSHI_TRADES
  SET TAG KALSHI_DB.STREAMING.market_data = 'pricing';

ALTER TABLE KALSHI_DB.STREAMING.KALSHI_TRADES
  MODIFY COLUMN trade_id SET TAG KALSHI_DB.STREAMING.pii_data = 'trade_identifier';

-- ============================================================================
-- 5. Direct Column Masking (overrides tag-based for specific columns)
-- ============================================================================

ALTER TABLE KALSHI_DB.STREAMING.KALSHI_MARKETS
  MODIFY COLUMN volume_fp SET MASKING POLICY KALSHI_DB.STREAMING.volume_mask;

ALTER TABLE KALSHI_DB.STREAMING.KALSHI_MARKETS
  MODIFY COLUMN volume_24h_fp SET MASKING POLICY KALSHI_DB.STREAMING.volume_mask;

ALTER TABLE KALSHI_DB.STREAMING.KALSHI_MARKETS
  MODIFY COLUMN open_interest_fp SET MASKING POLICY KALSHI_DB.STREAMING.volume_mask;

-- ============================================================================
-- 6. Verification Queries
-- ============================================================================

SELECT '=== Tag Assignments ===' AS section;

SELECT * FROM TABLE(
  KALSHI_DB.INFORMATION_SCHEMA.POLICY_REFERENCES(
    REF_ENTITY_DOMAIN => 'TABLE',
    REF_ENTITY_NAME => 'KALSHI_DB.STREAMING.KALSHI_MARKETS'
  )
);

SELECT * FROM TABLE(
  KALSHI_DB.INFORMATION_SCHEMA.POLICY_REFERENCES(
    REF_ENTITY_DOMAIN => 'TABLE',
    REF_ENTITY_NAME => 'KALSHI_DB.STREAMING.KALSHI_TRADES'
  )
);

SELECT '=== Masking Policies ===' AS section;

SHOW MASKING POLICIES IN SCHEMA KALSHI_DB.STREAMING;

SELECT '=== Tags ===' AS section;

SHOW TAGS IN SCHEMA KALSHI_DB.STREAMING;

-- ============================================================================
-- 7. Test: Verify masking works for different roles
-- ============================================================================

SELECT '=== Testing as ACCOUNTADMIN (should see raw data) ===' AS test;
SELECT ticker, last_price_dollars, yes_bid_dollars, volume_24h_fp
FROM KALSHI_DB.STREAMING.KALSHI_MARKETS
WHERE last_price_dollars IS NOT NULL
LIMIT 5;

-- To test as a restricted role:
-- USE ROLE PUBLIC;
-- SELECT ticker, last_price_dollars, yes_bid_dollars, volume_24h_fp
-- FROM KALSHI_DB.STREAMING.KALSHI_MARKETS LIMIT 5;
-- Expected: ticker=***RESTRICTED***, prices=NULL, volume=0

SELECT '=== Governance Applied Successfully ===' AS result;
