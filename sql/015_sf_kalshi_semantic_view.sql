-- ============================================================================
-- 015_sf_kalshi_semantic_view.sql
-- Semantic View for Kalshi Trading Data (Cortex Analyst)
-- Creates KALSHI_DB.STREAMING.KALSHI_ANALYTICS semantic view
-- Run via: snow sql -f sql/015_sf_kalshi_semantic_view.sql --connection $SNOW_CONNECTION
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE KALSHI_DB;
USE SCHEMA STREAMING;
USE WAREHOUSE INGEST;

CREATE OR REPLACE SEMANTIC VIEW KALSHI_DB.STREAMING.KALSHI_ANALYTICS
  TABLES (
    markets AS KALSHI_DB.STREAMING.KALSHI_MARKETS
      PRIMARY KEY (ticker),
    trades AS KALSHI_DB.STREAMING.KALSHI_TRADES
      PRIMARY KEY (trade_id),
    series AS KALSHI_DB.STREAMING.KALSHI_SERIES
      PRIMARY KEY (ticker)
  )
  RELATIONSHIPS (
    trades_to_markets AS trades(ticker) REFERENCES markets,
    markets_to_series AS markets(event_ticker) REFERENCES series
  )
  FACTS (
    markets.last_price AS markets.last_price_dollars
      WITH SYNONYMS = ('price', 'current price', 'last trade price'),
    markets.yes_bid AS markets.yes_bid_dollars
      WITH SYNONYMS = ('yes bid price'),
    markets.yes_ask AS markets.yes_ask_dollars
      WITH SYNONYMS = ('yes ask price'),
    markets.no_bid AS markets.no_bid_dollars
      WITH SYNONYMS = ('no bid price'),
    markets.no_ask AS markets.no_ask_dollars
      WITH SYNONYMS = ('no ask price'),
    markets.volume AS markets.volume_fp
      WITH SYNONYMS = ('total volume', 'contracts traded'),
    markets.volume_24h AS markets.volume_24h_fp
      WITH SYNONYMS = ('24 hour volume', 'daily volume', 'today volume'),
    markets.open_interest AS markets.open_interest_fp
      WITH SYNONYMS = ('OI', 'outstanding contracts', 'positions'),
    markets.liquidity AS markets.liquidity_dollars
      WITH SYNONYMS = ('market depth', 'available liquidity'),
    trades.yes_price AS trades.yes_price_dollars
      WITH SYNONYMS = ('yes trade price'),
    trades.no_price AS trades.no_price_dollars
      WITH SYNONYMS = ('no trade price'),
    trades.quantity AS trades.count_fp
      WITH SYNONYMS = ('contracts', 'trade size', 'lot size', 'qty')
  )
  DIMENSIONS (
    markets.market_ticker AS markets.ticker
      WITH SYNONYMS = ('ticker', 'market id', 'contract'),
    markets.market_title AS markets.title
      WITH SYNONYMS = ('name', 'market name', 'contract name', 'question'),
    markets.market_status AS markets.status
      WITH SYNONYMS = ('state', 'market state'),
    markets.market_type AS markets.market_type
      WITH SYNONYMS = ('type', 'contract type'),
    markets.result AS markets.result
      WITH SYNONYMS = ('outcome', 'settlement'),
    markets.strike_type AS markets.strike_type,
    trades.trade_id AS trades.trade_id
      WITH SYNONYMS = ('transaction id', 'tx id'),
    trades.taker_side AS trades.taker_side
      WITH SYNONYMS = ('side', 'direction', 'yes or no'),
    trades.trade_time AS trades.created_time
      WITH SYNONYMS = ('trade date', 'traded at', 'trade timestamp', 'when traded'),
    markets.close_time AS markets.close_time
      WITH SYNONYMS = ('closes at', 'closing time', 'expiry'),
    markets.open_time AS markets.open_time
      WITH SYNONYMS = ('opened at', 'listing date'),
    markets.created_time AS markets.created_time
      WITH SYNONYMS = ('market created', 'listed at'),
    trades.ingested_at AS trades.ingested_at
      WITH SYNONYMS = ('loaded at', 'streamed at'),
    series.series_ticker AS series.ticker
      WITH SYNONYMS = ('series id', 'event ticker'),
    series.series_title AS series.title
      WITH SYNONYMS = ('series name', 'event name'),
    series.category AS series.category
      WITH SYNONYMS = ('market category', 'topic', 'sector'),
    series.frequency AS series.frequency
  )
  METRICS (
    markets.total_volume AS SUM(markets.volume_fp)
      WITH SYNONYMS = ('total contracts traded', 'aggregate volume'),
    markets.total_volume_24h AS SUM(markets.volume_24h_fp)
      WITH SYNONYMS = ('total daily volume', 'total 24h volume'),
    markets.avg_price AS AVG(markets.last_price_dollars)
      WITH SYNONYMS = ('average price', 'mean price'),
    markets.total_open_interest AS SUM(markets.open_interest_fp)
      WITH SYNONYMS = ('total OI', 'total outstanding'),
    markets.total_liquidity AS SUM(markets.liquidity_dollars)
      WITH SYNONYMS = ('total market depth'),
    markets.market_count AS COUNT(*)
      WITH SYNONYMS = ('number of markets', 'how many markets'),
    markets.active_market_count AS COUNT_IF(markets.status = 'active')
      WITH SYNONYMS = ('active markets', 'open markets'),
    trades.trade_count AS COUNT(*)
      WITH SYNONYMS = ('number of trades', 'total trades', 'how many trades'),
    trades.total_contracts AS SUM(trades.count_fp)
      WITH SYNONYMS = ('total quantity', 'contracts exchanged'),
    trades.avg_yes_price AS AVG(trades.yes_price_dollars)
      WITH SYNONYMS = ('average yes price', 'mean yes'),
    trades.avg_no_price AS AVG(trades.no_price_dollars)
      WITH SYNONYMS = ('average no price', 'mean no')
  )
  COMMENT = 'Semantic view for Kalshi prediction market analytics — powers Cortex Analyst'
  AI_VERIFIED_QUERIES (
    top_markets_by_volume AS (
      QUESTION 'What are the top 10 markets by 24-hour volume?'
      ONBOARDING_QUESTION TRUE
      SQL $$
        SELECT ticker, title, status, last_price_dollars, volume_24h_fp, open_interest_fp
        FROM KALSHI_DB.STREAMING.KALSHI_MARKETS
        WHERE status = 'active'
        ORDER BY volume_24h_fp DESC NULLS LAST
        LIMIT 10
      $$
    ),
    recent_trades AS (
      QUESTION 'Show me the most recent 20 trades'
      ONBOARDING_QUESTION TRUE
      SQL $$
        SELECT trade_id, ticker, taker_side, yes_price_dollars, no_price_dollars,
               count_fp, created_time
        FROM KALSHI_DB.STREAMING.KALSHI_TRADES
        ORDER BY created_time DESC
        LIMIT 20
      $$
    ),
    trades_today AS (
      QUESTION 'How many trades happened today?'
      SQL $$
        SELECT COUNT(*) AS trade_count,
               SUM(count_fp) AS total_contracts,
               AVG(yes_price_dollars) AS avg_yes_price
        FROM KALSHI_DB.STREAMING.KALSHI_TRADES
        WHERE created_time >= CURRENT_DATE()
      $$
    ),
    volume_by_category AS (
      QUESTION 'What is the total volume by category?'
      ONBOARDING_QUESTION TRUE
      SQL $$
        SELECT s.category, COUNT(m.ticker) AS market_count,
               SUM(m.volume_fp) AS total_volume,
               SUM(m.open_interest_fp) AS total_oi
        FROM KALSHI_DB.STREAMING.KALSHI_MARKETS m
        JOIN KALSHI_DB.STREAMING.KALSHI_SERIES s ON m.event_ticker = s.ticker
        WHERE m.status = 'active'
        GROUP BY s.category
        ORDER BY total_volume DESC NULLS LAST
      $$
    ),
    spread_analysis AS (
      QUESTION 'Which active markets have the widest bid-ask spread?'
      SQL $$
        SELECT ticker, title, yes_ask_dollars - yes_bid_dollars AS spread,
               yes_bid_dollars, yes_ask_dollars, volume_24h_fp
        FROM KALSHI_DB.STREAMING.KALSHI_MARKETS
        WHERE status = 'active'
          AND yes_bid_dollars IS NOT NULL
          AND yes_ask_dollars IS NOT NULL
        ORDER BY spread DESC
        LIMIT 20
      $$
    )
  );

-- Grant access
GRANT SELECT ON SEMANTIC VIEW KALSHI_DB.STREAMING.KALSHI_ANALYTICS TO ROLE PUBLIC;

-- Verification
DESCRIBE SEMANTIC VIEW KALSHI_DB.STREAMING.KALSHI_ANALYTICS;
SHOW SEMANTIC VIEWS IN SCHEMA KALSHI_DB.STREAMING;
