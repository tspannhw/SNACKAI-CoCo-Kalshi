-- ============================================================================
-- 001_pg_kalshi_tables.sql
-- PostgreSQL DDL for Kalshi Trading Data Pipeline
-- Target: Snowflake Postgres instance
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Series Table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kalshi_series (
    ticker          TEXT PRIMARY KEY,
    title           TEXT NOT NULL,
    category        TEXT,
    frequency       TEXT,
    tags            TEXT[],
    fee_type        TEXT,
    fee_multiplier  INTEGER DEFAULT 1,
    contract_url    TEXT,
    settlement_sources JSONB,
    last_updated_ts TIMESTAMPTZ,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_series_category ON kalshi_series (category);
CREATE INDEX IF NOT EXISTS idx_series_ingested ON kalshi_series (ingested_at);

COMMENT ON TABLE kalshi_series IS
    'Kalshi prediction market series metadata';

-- ----------------------------------------------------------------------------
-- 2. Markets Table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kalshi_markets (
    ticker              TEXT PRIMARY KEY,
    event_ticker        TEXT,
    title               TEXT,
    status              TEXT,
    market_type         TEXT,
    close_time          TIMESTAMPTZ,
    expiration_time     TIMESTAMPTZ,
    expected_expiration_time TIMESTAMPTZ,
    open_time           TIMESTAMPTZ,
    last_price_dollars  NUMERIC(10,4),
    yes_bid_dollars     NUMERIC(10,4),
    yes_ask_dollars     NUMERIC(10,4),
    no_bid_dollars      NUMERIC(10,4),
    no_ask_dollars      NUMERIC(10,4),
    volume_fp           NUMERIC(20,2),
    volume_24h_fp       NUMERIC(20,2),
    open_interest_fp    NUMERIC(20,2),
    liquidity_dollars   NUMERIC(20,4),
    result              TEXT,
    notional_value_dollars NUMERIC(10,4),
    yes_sub_title       TEXT,
    no_sub_title        TEXT,
    can_close_early     BOOLEAN,
    expiration_value    TEXT,
    strike_type         TEXT,
    rules_primary       TEXT,
    created_time        TIMESTAMPTZ,
    updated_time        TIMESTAMPTZ,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_markets_event ON kalshi_markets (event_ticker);
CREATE INDEX IF NOT EXISTS idx_markets_status ON kalshi_markets (status);
CREATE INDEX IF NOT EXISTS idx_markets_close ON kalshi_markets (close_time);
CREATE INDEX IF NOT EXISTS idx_markets_ingested ON kalshi_markets (ingested_at);

COMMENT ON TABLE kalshi_markets IS
    'Kalshi prediction market listings with pricing data';

-- ----------------------------------------------------------------------------
-- 3. Trades Table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kalshi_trades (
    trade_id            TEXT PRIMARY KEY,
    ticker              TEXT NOT NULL,
    taker_side          TEXT,
    yes_price_dollars   NUMERIC(10,4),
    no_price_dollars    NUMERIC(10,4),
    count_fp            NUMERIC(20,2),
    created_time        TIMESTAMPTZ NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trades_ticker ON kalshi_trades (ticker);
CREATE INDEX IF NOT EXISTS idx_trades_created ON kalshi_trades (created_time);
CREATE INDEX IF NOT EXISTS idx_trades_ingested ON kalshi_trades (ingested_at);

COMMENT ON TABLE kalshi_trades IS
    'Kalshi trade execution records';

-- ----------------------------------------------------------------------------
-- 4. Categories & Tags Reference Table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kalshi_categories_tags (
    category        TEXT NOT NULL,
    tag             TEXT NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (category, tag)
);

COMMENT ON TABLE kalshi_categories_tags IS
    'Kalshi market categories and associated tags';

-- ----------------------------------------------------------------------------
-- 5. Sports Filters Reference Table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kalshi_sports_filters (
    filter_id       BIGSERIAL PRIMARY KEY,
    sport           TEXT NOT NULL,
    scope           TEXT NOT NULL,
    competition     TEXT DEFAULT '',
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sports_filters_unique
    ON kalshi_sports_filters (sport, scope, competition);

COMMENT ON TABLE kalshi_sports_filters IS
    'Kalshi sports market filter metadata';

-- ----------------------------------------------------------------------------
-- 6. Ingestion Log Table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kalshi_ingestion_log (
    log_id          BIGSERIAL PRIMARY KEY,
    endpoint        TEXT NOT NULL,
    records_fetched INTEGER DEFAULT 0,
    records_upserted INTEGER DEFAULT 0,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'success', 'error')),
    error_message   TEXT,
    duration_ms     INTEGER
);

CREATE INDEX IF NOT EXISTS idx_ingestion_log_status
    ON kalshi_ingestion_log (status, started_at);

COMMENT ON TABLE kalshi_ingestion_log IS
    'Tracks each Kalshi API ingestion run';

-- ----------------------------------------------------------------------------
-- Verification
-- ----------------------------------------------------------------------------
SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'kalshi%' ORDER BY tablename;
