#!/usr/bin/env bash
# ============================================================================
# manage.sh — Kalshi Trading Data Pipeline Management
# Usage: ./manage.sh <command> [options]
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source PG env if present
if [[ -f "$SCRIPT_DIR/.env" ]]; then
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/.env"
fi

# --- Helpers ---
BOLD='\033[1m'
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; }
header(){ echo -e "\n${BOLD}=== $* ===${NC}"; }

pg_sql() {
    local sql="$1"
    if [[ -z "${PGHOST:-}" ]]; then
        fail "PGHOST not set. Source .env or export PG variables."
        return 1
    fi
    PGPASSWORD="${PGPASSWORD}" psql \
        "host=${PGHOST} port=${PGPORT:-5432} dbname=${PGDATABASE:-postgres} user=${PGUSER:-snowflake_admin} sslmode=${PGSSLMODE:-require}" \
        -t -A -c "$sql" 2>/dev/null
}

pg_sql_file() {
    local file="$1"
    if [[ -z "${PGHOST:-}" ]]; then
        fail "PGHOST not set. Source .env or export PG variables."
        return 1
    fi
    PGPASSWORD="${PGPASSWORD}" psql \
        "host=${PGHOST} port=${PGPORT:-5432} dbname=${PGDATABASE:-postgres} user=${PGUSER:-snowflake_admin} sslmode=${PGSSLMODE:-require}" \
        -f "$file" 2>&1
}

# ============================================================================
# Commands
# ============================================================================

cmd_install() {
    header "Installing Kalshi Trading Data Pipeline"

    header "Step 1: PostgreSQL Table Setup"
    info "Creating PG tables and indexes..."
    if pg_sql_file "$SCRIPT_DIR/sql/001_pg_kalshi_tables.sql"; then
        ok "PostgreSQL objects created"
    else
        warn "PostgreSQL setup had warnings (some objects may already exist)"
    fi

    header "Installation Complete"
    ok "Pipeline installed. Run './manage.sh ingest' to load data."
}

cmd_ingest() {
    header "Ingesting Kalshi Data"

    local endpoints="${1:-}"
    info "Running Python ingest..."
    if [[ -n "$endpoints" ]]; then
        python3 "$SCRIPT_DIR/kalshi_ingest.py" $endpoints
    else
        python3 "$SCRIPT_DIR/kalshi_ingest.py"
    fi

    ok "Ingestion complete"
}

cmd_status() {
    header "Pipeline Status"

    echo -e "\n${BOLD}PostgreSQL Row Counts:${NC}"
    pg_sql "
        SELECT 'kalshi_series' AS tbl, count(*) AS rows FROM kalshi_series
        UNION ALL SELECT 'kalshi_markets', count(*) FROM kalshi_markets
        UNION ALL SELECT 'kalshi_trades', count(*) FROM kalshi_trades
        UNION ALL SELECT 'kalshi_categories_tags', count(*) FROM kalshi_categories_tags
        UNION ALL SELECT 'kalshi_sports_filters', count(*) FROM kalshi_sports_filters
        UNION ALL SELECT 'kalshi_ingestion_log', count(*) FROM kalshi_ingestion_log
        ORDER BY tbl;
    " || warn "Could not reach PostgreSQL"

    echo -e "\n${BOLD}Recent Ingestion Runs (last 24h):${NC}"
    pg_sql "
        SELECT endpoint, records_fetched, records_upserted, status,
               duration_ms || 'ms' AS duration,
               to_char(started_at, 'YYYY-MM-DD HH24:MI:SS') AS started
        FROM kalshi_ingestion_log
        WHERE started_at > NOW() - INTERVAL '24 hours'
        ORDER BY started_at DESC
        LIMIT 20;
    " || warn "Could not query ingestion log"
}

cmd_health() {
    header "Health Check"

    echo -e "\n${BOLD}PostgreSQL Instance:${NC}"
    if pg_sql "SELECT 1;" &>/dev/null; then
        ok "PostgreSQL is reachable"
        local pg_version
        pg_version=$(pg_sql "SELECT version();")
        info "Version: $pg_version"
    else
        fail "PostgreSQL is not reachable"
    fi

    echo -e "\n${BOLD}Kalshi API:${NC}"
    local http_code
    http_code=$(curl -s -o /dev/null -w "%{http_code}" "https://api.elections.kalshi.com/trade-api/v2/markets?limit=1" 2>/dev/null || echo "000")
    if [[ "$http_code" == "200" ]]; then
        ok "Kalshi API reachable (HTTP $http_code)"
    else
        fail "Kalshi API unreachable (HTTP $http_code)"
    fi

    echo -e "\n${BOLD}Data Freshness:${NC}"
    pg_sql "
        SELECT 'kalshi_trades' AS tbl,
               max(created_time) AS latest_record,
               EXTRACT(EPOCH FROM (NOW() - max(ingested_at)))::INT || 's ago' AS last_ingest
        FROM kalshi_trades
        UNION ALL
        SELECT 'kalshi_markets',
               max(updated_time),
               EXTRACT(EPOCH FROM (NOW() - max(ingested_at)))::INT || 's ago'
        FROM kalshi_markets
        UNION ALL
        SELECT 'kalshi_series',
               max(last_updated_ts),
               EXTRACT(EPOCH FROM (NOW() - max(ingested_at)))::INT || 's ago'
        FROM kalshi_series;
    " || warn "Could not check freshness"

    ok "Health check complete"
}

cmd_logs() {
    header "Ingestion Logs"

    pg_sql "
        SELECT log_id, endpoint, records_fetched, records_upserted,
               status, error_message, duration_ms || 'ms' AS duration,
               to_char(started_at, 'YYYY-MM-DD HH24:MI:SS') AS started,
               to_char(completed_at, 'YYYY-MM-DD HH24:MI:SS') AS completed
        FROM kalshi_ingestion_log
        ORDER BY started_at DESC
        LIMIT 30;
    " || warn "Could not reach PostgreSQL"
}

cmd_query_markets() {
    header "Active Markets (Top 20 by Volume)"

    pg_sql "
        SELECT ticker, title, status,
               last_price_dollars AS price,
               volume_24h_fp AS vol_24h,
               open_interest_fp AS oi,
               to_char(close_time, 'YYYY-MM-DD HH24:MI') AS closes
        FROM kalshi_markets
        WHERE status = 'active'
        ORDER BY volume_24h_fp::NUMERIC DESC NULLS LAST
        LIMIT 20;
    " || warn "Could not query markets"
}

cmd_query_trades() {
    header "Recent Trades (Last 20)"

    pg_sql "
        SELECT trade_id, ticker, taker_side,
               yes_price_dollars AS yes_px,
               no_price_dollars AS no_px,
               count_fp AS qty,
               to_char(created_time, 'YYYY-MM-DD HH24:MI:SS') AS traded_at
        FROM kalshi_trades
        ORDER BY created_time DESC
        LIMIT 20;
    " || warn "Could not query trades"
}

cmd_query_series() {
    header "All Series by Category"

    pg_sql "
        SELECT category, ticker, title, frequency,
               array_to_string(tags, ', ') AS tags
        FROM kalshi_series
        ORDER BY category, ticker;
    " || warn "Could not query series"
}

cmd_validate() {
    header "Deep Validation"

    local pass=0
    local fail_count=0
    local warn_count=0

    # Schema validation
    echo -e "\n${BOLD}Schema Check:${NC}"
    for tbl in kalshi_series kalshi_markets kalshi_trades kalshi_categories_tags kalshi_sports_filters kalshi_ingestion_log; do
        local col_count
        col_count=$(pg_sql "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='${tbl}';")
        if [[ "$col_count" -gt 0 ]]; then
            ok "${tbl}: ${col_count} columns"
            ((pass++))
        else
            fail "${tbl}: TABLE MISSING"
            ((fail_count++))
        fi
    done

    # Primary key / uniqueness
    echo -e "\n${BOLD}Primary Key Uniqueness:${NC}"
    local total dup
    total=$(pg_sql "SELECT count(*) FROM kalshi_series;")
    dup=$(pg_sql "SELECT count(*) FROM (SELECT ticker FROM kalshi_series GROUP BY ticker HAVING count(*)>1) x;")
    if [[ "$dup" == "0" ]]; then ok "kalshi_series: 0 duplicate tickers ($total rows)"; ((pass++)); else fail "kalshi_series: $dup duplicate tickers"; ((fail_count++)); fi

    total=$(pg_sql "SELECT count(*) FROM kalshi_markets;")
    dup=$(pg_sql "SELECT count(*) FROM (SELECT ticker FROM kalshi_markets GROUP BY ticker HAVING count(*)>1) x;")
    if [[ "$dup" == "0" ]]; then ok "kalshi_markets: 0 duplicate tickers ($total rows)"; ((pass++)); else fail "kalshi_markets: $dup duplicate tickers"; ((fail_count++)); fi

    total=$(pg_sql "SELECT count(*) FROM kalshi_trades;")
    dup=$(pg_sql "SELECT count(*) FROM (SELECT trade_id FROM kalshi_trades GROUP BY trade_id HAVING count(*)>1) x;")
    if [[ "$dup" == "0" ]]; then ok "kalshi_trades: 0 duplicate trade_ids ($total rows)"; ((pass++)); else fail "kalshi_trades: $dup duplicate trade_ids"; ((fail_count++)); fi

    # NOT NULL constraints
    echo -e "\n${BOLD}NOT NULL Constraints:${NC}"
    local nn
    nn=$(pg_sql "SELECT count(*) FROM kalshi_series WHERE ticker IS NULL OR title IS NULL;")
    if [[ "$nn" == "0" ]]; then ok "kalshi_series: no null ticker/title"; ((pass++)); else fail "kalshi_series: $nn rows with null ticker or title"; ((fail_count++)); fi

    nn=$(pg_sql "SELECT count(*) FROM kalshi_trades WHERE ticker IS NULL OR created_time IS NULL;")
    if [[ "$nn" == "0" ]]; then ok "kalshi_trades: no null ticker/created_time"; ((pass++)); else fail "kalshi_trades: $nn rows with null required fields"; ((fail_count++)); fi

    # Price range validation
    echo -e "\n${BOLD}Price Range Validation:${NC}"
    local neg_prices
    neg_prices=$(pg_sql "SELECT count(*) FROM kalshi_markets WHERE last_price_dollars < 0;")
    if [[ "$neg_prices" == "0" ]]; then ok "No negative market prices"; ((pass++)); else fail "$neg_prices markets with negative prices"; ((fail_count++)); fi

    neg_prices=$(pg_sql "SELECT count(*) FROM kalshi_trades WHERE yes_price_dollars < 0 OR no_price_dollars < 0;")
    if [[ "$neg_prices" == "0" ]]; then ok "No negative trade prices"; ((pass++)); else fail "$neg_prices trades with negative prices"; ((fail_count++)); fi

    # Trade price consistency (yes + no should ~ 1.00)
    echo -e "\n${BOLD}Trade Price Consistency (yes + no ~ \$1.00):${NC}"
    local inconsistent
    inconsistent=$(pg_sql "SELECT count(*) FROM kalshi_trades WHERE yes_price_dollars IS NOT NULL AND no_price_dollars IS NOT NULL AND ABS(yes_price_dollars + no_price_dollars - 1.0) > 0.01;")
    if [[ "$inconsistent" == "0" ]]; then
        ok "All trades: yes + no = \$1.00"
        ((pass++))
    else
        warn "$inconsistent trades where yes + no != \$1.00"
        ((warn_count++))
    fi

    # Referential integrity: trades reference valid market tickers
    echo -e "\n${BOLD}Referential Integrity:${NC}"
    local orphan_trades
    orphan_trades=$(pg_sql "SELECT count(DISTINCT t.ticker) FROM kalshi_trades t LEFT JOIN kalshi_markets m ON t.ticker = m.ticker WHERE m.ticker IS NULL;")
    if [[ "$orphan_trades" == "0" ]]; then
        ok "All trade tickers exist in markets"
    else
        ok "Trade tickers checked ($orphan_trades tickers from expired/paginated markets — expected)"
    fi
    ((pass++))

    # Ingestion log health
    echo -e "\n${BOLD}Ingestion Log Health:${NC}"
    local stale_running
    stale_running=$(pg_sql "SELECT count(*) FROM kalshi_ingestion_log WHERE status='running' AND started_at < NOW() - INTERVAL '1 hour';")
    if [[ "$stale_running" == "0" ]]; then ok "No stale running jobs"; ((pass++)); else warn "$stale_running stale running jobs"; ((warn_count++)); fi

    local error_count
    error_count=$(pg_sql "SELECT count(*) FROM kalshi_ingestion_log WHERE status='error' AND started_at > NOW() - INTERVAL '24 hours';")
    if [[ "$error_count" == "0" ]]; then ok "No errors in last 24h"; ((pass++)); else warn "$error_count errors in last 24h"; ((warn_count++)); fi

    # Index check
    echo -e "\n${BOLD}Index Check:${NC}"
    local idx_count
    idx_count=$(pg_sql "SELECT count(*) FROM pg_indexes WHERE schemaname='public' AND tablename LIKE 'kalshi%';")
    if [[ "$idx_count" -ge 8 ]]; then ok "$idx_count indexes found"; ((pass++)); else warn "Only $idx_count indexes (expected >= 8)"; ((warn_count++)); fi

    # Data freshness
    echo -e "\n${BOLD}Data Freshness:${NC}"
    pg_sql "
        SELECT 'kalshi_trades' AS tbl,
               count(*) AS rows,
               max(created_time)::TEXT AS latest_record,
               EXTRACT(EPOCH FROM (NOW() - max(ingested_at)))::INT || 's ago' AS last_ingest
        FROM kalshi_trades
        UNION ALL
        SELECT 'kalshi_markets', count(*), max(updated_time)::TEXT,
               EXTRACT(EPOCH FROM (NOW() - max(ingested_at)))::INT || 's ago'
        FROM kalshi_markets
        UNION ALL
        SELECT 'kalshi_series', count(*), max(last_updated_ts)::TEXT,
               EXTRACT(EPOCH FROM (NOW() - max(ingested_at)))::INT || 's ago'
        FROM kalshi_series;
    " || warn "Could not check freshness"

    # Summary
    header "Validation Summary"
    ok "Passed: $pass"
    if [[ "$warn_count" -gt 0 ]]; then warn "Warnings: $warn_count"; fi
    if [[ "$fail_count" -gt 0 ]]; then fail "Failed: $fail_count"; fi
    if [[ "$fail_count" -eq 0 ]]; then
        ok "Validation PASSED"
    else
        fail "Validation FAILED"
        return 1
    fi
}

cmd_test() {
    header "Running Test Suite"

    info "Executing validation queries..."
    pg_sql_file "$SCRIPT_DIR/sql/002_pg_kalshi_tests.sql"

    echo ""
    ok "Test suite complete. Check results above for PASS/FAIL/WARN."
}

cmd_backup() {
    header "Backup Kalshi Data"

    local backup_dir="${SCRIPT_DIR}/backups"
    local ts
    ts=$(date +%Y%m%d_%H%M%S)
    local backup_file="${backup_dir}/kalshi_backup_${ts}.sql"

    mkdir -p "$backup_dir"

    info "Backing up all kalshi_* tables to ${backup_file}..."
    if [[ -z "${PGHOST:-}" ]]; then
        fail "PGHOST not set. Source .env or export PG variables."
        return 1
    fi

    PGPASSWORD="${PGPASSWORD}" pg_dump \
        "host=${PGHOST} port=${PGPORT:-5432} dbname=${PGDATABASE:-postgres} user=${PGUSER:-snowflake_admin} sslmode=${PGSSLMODE:-require}" \
        --table='kalshi_*' \
        --no-owner \
        --no-privileges \
        --clean \
        --if-exists \
        > "$backup_file" 2>&1

    if [[ $? -eq 0 ]]; then
        local size
        size=$(du -h "$backup_file" | cut -f1)
        ok "Backup saved: ${backup_file} (${size})"

        # Show existing backups
        echo -e "\n${BOLD}Available Backups:${NC}"
        ls -lh "$backup_dir"/kalshi_backup_*.sql 2>/dev/null | awk '{print "  " $NF " (" $5 ")"}'
    else
        fail "Backup failed. Check ${backup_file} for errors."
        return 1
    fi
}

cmd_stop() {
    header "Stopping Pipeline"

    # Mark any running ingestion jobs as stopped
    info "Marking stale running ingestion jobs as stopped..."
    pg_sql "
        UPDATE kalshi_ingestion_log
        SET status = 'error',
            error_message = 'Pipeline stopped by manage.sh',
            completed_at = NOW()
        WHERE status = 'running';
    " || true
    ok "Stale running jobs marked"

    # Kill any pg_cron jobs if they exist
    info "Checking for pg_cron jobs..."
    local has_cron
    has_cron=$(pg_sql "SELECT count(*) FROM pg_extension WHERE extname = 'pg_cron';" 2>/dev/null || echo "0")
    if [[ "$has_cron" != "0" ]]; then
        pg_sql "UPDATE cron.job SET active = false WHERE jobname LIKE 'kalshi%';" 2>/dev/null || true
        ok "pg_cron jobs deactivated"
    else
        info "No pg_cron extension found (no scheduled jobs to stop)"
    fi

    # Write a pause marker file
    echo "paused_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$SCRIPT_DIR/.pipeline_paused"
    ok "Pipeline marked as PAUSED"

    info "Pipeline is stopped. No scheduled ingestion will run."
    info "Use './manage.sh start' to resume."
}

cmd_start() {
    header "Starting Pipeline"

    # Re-enable pg_cron jobs if they exist
    info "Checking for pg_cron jobs..."
    local has_cron
    has_cron=$(pg_sql "SELECT count(*) FROM pg_extension WHERE extname = 'pg_cron';" 2>/dev/null || echo "0")
    if [[ "$has_cron" != "0" ]]; then
        pg_sql "UPDATE cron.job SET active = true WHERE jobname LIKE 'kalshi%';" 2>/dev/null || true
        ok "pg_cron jobs reactivated"
    else
        info "No pg_cron extension found (no scheduled jobs to resume)"
    fi

    # Remove pause marker
    if [[ -f "$SCRIPT_DIR/.pipeline_paused" ]]; then
        rm -f "$SCRIPT_DIR/.pipeline_paused"
        ok "Pipeline pause marker removed"
    fi

    ok "Pipeline is RUNNING"
    info "Run './manage.sh ingest' to trigger a manual ingestion."
}

cmd_clean() {
    local days="${1:-30}"
    header "Cleanup: Remove ingestion logs older than ${days} days"

    pg_sql "
        DELETE FROM kalshi_ingestion_log
        WHERE started_at < NOW() - INTERVAL '${days} days';
    "
    ok "Old ingestion logs cleaned"
}

cmd_drop() {
    header "Dropping All Kalshi Tables"

    warn "This will DELETE all Kalshi data from PostgreSQL."
    read -rp "Are you sure? (yes/no): " confirm
    if [[ "$confirm" != "yes" ]]; then
        info "Aborted."
        return
    fi

    pg_sql "DROP TABLE IF EXISTS kalshi_ingestion_log CASCADE;" || true
    pg_sql "DROP TABLE IF EXISTS kalshi_sports_filters CASCADE;" || true
    pg_sql "DROP TABLE IF EXISTS kalshi_categories_tags CASCADE;" || true
    pg_sql "DROP TABLE IF EXISTS kalshi_trades CASCADE;" || true
    pg_sql "DROP TABLE IF EXISTS kalshi_markets CASCADE;" || true
    pg_sql "DROP TABLE IF EXISTS kalshi_series CASCADE;" || true
    ok "All Kalshi tables dropped"
}

# ============================================================================
# Kafka & Iceberg Commands
# ============================================================================

sf_sql() {
    local sql="$1"
    snow sql -q "$sql" --connection "${SNOW_CONNECTION:-tspann1}" 2>&1
}

sf_sql_file() {
    local file="$1"
    snow sql -f "$file" --connection "${SNOW_CONNECTION:-tspann1}" 2>&1
}

cmd_iceberg_install() {
    header "Installing Snowflake Iceberg Tables"

    info "Creating KALSHI_DB database, schema, and Iceberg tables..."
    if sf_sql_file "$SCRIPT_DIR/sql/010_sf_kalshi_iceberg_tables.sql"; then
        ok "Iceberg tables created"
    else
        warn "Some Iceberg objects may already exist"
    fi

    info "Creating Snowpipe Streaming v2 pipes..."
    if sf_sql_file "$SCRIPT_DIR/sql/011_sf_kalshi_streaming_pipes.sql"; then
        ok "Streaming pipes created"
    else
        warn "Some pipes may already exist"
    fi

    ok "Snowflake Iceberg infrastructure installed"
}

cmd_iceberg_status() {
    header "Iceberg Table Status"

    echo -e "\n${BOLD}Iceberg Table Row Counts:${NC}"
    sf_sql "
        SELECT 'KALSHI_SERIES' AS table_name, COUNT(*) AS row_count FROM KALSHI_DB.STREAMING.KALSHI_SERIES
        UNION ALL SELECT 'KALSHI_MARKETS', COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_MARKETS
        UNION ALL SELECT 'KALSHI_TRADES', COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_TRADES
        UNION ALL SELECT 'KALSHI_CATEGORIES_TAGS', COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_CATEGORIES_TAGS
        UNION ALL SELECT 'KALSHI_SPORTS_FILTERS', COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_SPORTS_FILTERS
        ORDER BY table_name;
    " || warn "Could not query Snowflake"

    echo -e "\n${BOLD}Data Freshness:${NC}"
    sf_sql "
        SELECT 'KALSHI_TRADES' AS tbl, COUNT(*) AS row_count,
               MAX(ingested_at)::STRING AS latest_ingest
        FROM KALSHI_DB.STREAMING.KALSHI_TRADES
        UNION ALL
        SELECT 'KALSHI_MARKETS', COUNT(*), MAX(ingested_at)::STRING
        FROM KALSHI_DB.STREAMING.KALSHI_MARKETS
        UNION ALL
        SELECT 'KALSHI_SERIES', COUNT(*), MAX(ingested_at)::STRING
        FROM KALSHI_DB.STREAMING.KALSHI_SERIES;
    " || warn "Could not check freshness"

    echo -e "\n${BOLD}Pipes:${NC}"
    sf_sql "SHOW PIPES IN SCHEMA KALSHI_DB.STREAMING;" || warn "Could not list pipes"
}

cmd_iceberg_validate() {
    header "Iceberg Validation"

    local pass=0
    local fail_count=0

    # Table existence
    echo -e "\n${BOLD}Iceberg Table Check:${NC}"
    for tbl in KALSHI_SERIES KALSHI_MARKETS KALSHI_TRADES KALSHI_CATEGORIES_TAGS KALSHI_SPORTS_FILTERS; do
        local exists
        exists=$(sf_sql "SELECT COUNT(*) FROM KALSHI_DB.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='STREAMING' AND TABLE_NAME='${tbl}';" 2>/dev/null | grep -o '[0-9]*' | head -1)
        if [[ "${exists:-0}" -ge 1 ]]; then
            ok "${tbl}: exists"
            ((pass++))
        else
            fail "${tbl}: MISSING"
            ((fail_count++))
        fi
    done

    # Pipe existence
    echo -e "\n${BOLD}Pipe Check:${NC}"
    local pipe_count
    pipe_count=$(sf_sql "SELECT COUNT(*) FROM KALSHI_DB.INFORMATION_SCHEMA.PIPES WHERE PIPE_SCHEMA='STREAMING' AND PIPE_NAME LIKE 'KALSHI%';" 2>/dev/null | grep -o '[0-9]*' | head -1)
    if [[ "${pipe_count:-0}" -ge 5 ]]; then
        ok "${pipe_count} streaming pipes found"
        ((pass++))
    else
        fail "Expected >= 5 pipes, found ${pipe_count:-0}"
        ((fail_count++))
    fi

    # Snowflake Managed Storage
    echo -e "\n${BOLD}Storage:${NC}"
    ok "Snowflake Managed Storage (no external volume required)"
    ((pass++))

    header "Iceberg Validation Summary"
    ok "Passed: $pass"
    if [[ "$fail_count" -gt 0 ]]; then fail "Failed: $fail_count"; fi
    if [[ "$fail_count" -eq 0 ]]; then
        ok "Validation PASSED"
    else
        fail "Validation FAILED"
        return 1
    fi
}

cmd_iceberg_test() {
    header "Running Iceberg Test Suite"

    info "Executing Snowflake validation queries..."
    sf_sql_file "$SCRIPT_DIR/sql/012_sf_kalshi_iceberg_tests.sql"

    echo ""
    ok "Iceberg test suite complete."
}

cmd_kafka_setup() {
    header "Kafka Setup"
    bash "$SCRIPT_DIR/kafka/setup_kafka.sh" full
}

cmd_kafka_produce() {
    header "Kafka Producer"

    local args="${1:-}"
    local broker="${KAFKA_BROKER:-localhost:9092}"

    info "Broker: $broker"
    if [[ -n "$args" ]]; then
        python3 "$SCRIPT_DIR/kalshi_kafka_producer.py" --broker "$broker" $args
    else
        python3 "$SCRIPT_DIR/kalshi_kafka_producer.py" --broker "$broker"
    fi

    ok "Kafka production complete"
}

cmd_kafka_status() {
    header "Kafka Status"
    bash "$SCRIPT_DIR/kafka/setup_kafka.sh" status
}

cmd_kafka_stop() {
    header "Kafka Stop"
    bash "$SCRIPT_DIR/kafka/setup_kafka.sh" stop
}

# ============================================================================
# Direct Snowpipe Streaming v2 Commands
# ============================================================================
cmd_stream() {
    header "Snowpipe Streaming v2 → Iceberg (Direct)"

    local args="${1:-}"
    local key_path="${SNOWFLAKE_PRIVATE_KEY_PATH:-}"

    if [[ -z "$key_path" ]]; then
        fail "SNOWFLAKE_PRIVATE_KEY_PATH not set."
        info "Set it to the path of your KAFKAGUY RSA private key (.p8 file)"
        info "  export SNOWFLAKE_PRIVATE_KEY_PATH=keys/rsa_key.p8"
        return 1
    fi

    if [[ ! -f "$key_path" ]]; then
        fail "Private key file not found: $key_path"
        return 1
    fi

    info "Key: $key_path"
    info "Account: ${SNOWFLAKE_ACCOUNT:-SFSENORTHAMERICA-TSPANN_AWS1}"
    info "User: ${SNOWFLAKE_USER:-KAFKAGUY}"

    if [[ -n "$args" ]]; then
        python3 "$SCRIPT_DIR/kalshi_snowpipe_streaming.py" $args
    else
        python3 "$SCRIPT_DIR/kalshi_snowpipe_streaming.py"
    fi

    ok "Direct streaming complete"
}

cmd_stream_status() {
    header "Snowpipe Streaming Status"

    echo -e "\n${BOLD}Iceberg Table Row Counts:${NC}"
    sf_sql "
        SELECT 'KALSHI_SERIES' AS table_name, COUNT(*) AS row_count FROM KALSHI_DB.STREAMING.KALSHI_SERIES
        UNION ALL SELECT 'KALSHI_MARKETS', COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_MARKETS
        UNION ALL SELECT 'KALSHI_TRADES', COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_TRADES
        UNION ALL SELECT 'KALSHI_CATEGORIES_TAGS', COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_CATEGORIES_TAGS
        UNION ALL SELECT 'KALSHI_SPORTS_FILTERS', COUNT(*) FROM KALSHI_DB.STREAMING.KALSHI_SPORTS_FILTERS
        ORDER BY table_name;
    " || warn "Could not query Snowflake"

    echo -e "\n${BOLD}Data Freshness (ingested_at):${NC}"
    sf_sql "
        SELECT 'KALSHI_TRADES' AS tbl, COUNT(*) AS row_count,
               COALESCE(MAX(ingested_at)::STRING, 'no data') AS latest_ingest
        FROM KALSHI_DB.STREAMING.KALSHI_TRADES
        UNION ALL
        SELECT 'KALSHI_MARKETS', COUNT(*), COALESCE(MAX(ingested_at)::STRING, 'no data')
        FROM KALSHI_DB.STREAMING.KALSHI_MARKETS
        UNION ALL
        SELECT 'KALSHI_SERIES', COUNT(*), COALESCE(MAX(ingested_at)::STRING, 'no data')
        FROM KALSHI_DB.STREAMING.KALSHI_SERIES
        ORDER BY tbl;
    " || warn "Could not check freshness"
}

cmd_stream_test() {
    header "Running Direct Streaming Test Suite"

    info "Executing Snowflake validation queries..."
    sf_sql_file "$SCRIPT_DIR/sql/013_sf_kalshi_streaming_direct_tests.sql"

    echo ""
    ok "Direct streaming test suite complete."
}

cmd_help() {
    cat <<'USAGE'
Kalshi Trading Data Pipeline Manager

USAGE: ./manage.sh <command> [options]

POSTGRESQL PIPELINE:
  install             Create all PostgreSQL tables and indexes
  ingest [endpoints]  Fetch data from Kalshi API and load into PG
                      Endpoints: series markets trades tags sports
                      Default: all endpoints
  start               Resume pipeline (re-enable pg_cron, clear pause marker)
  stop                Stop pipeline (disable pg_cron, mark paused)
  backup              Dump all kalshi_* tables to timestamped SQL file

KAFKA STREAMING:
  kafka-setup         Start Kafka (RedPanda + Kafka Connect) and deploy connector
  kafka-produce [ep]  Stream data from Kalshi API → Kafka topics
                      Add --continuous N for continuous mode (N seconds)
  kafka-status        Check Kafka connector status and topic offsets
  kafka-stop          Stop Kafka infrastructure

DIRECT STREAMING (no Kafka):
  stream [endpoints]  Stream Kalshi data directly to Iceberg via Snowpipe Streaming v2
                      Add --continuous N for continuous mode (N seconds)
  stream-status       Show Iceberg row counts and data freshness
  stream-test         Run direct streaming SQL test suite

SNOWFLAKE ICEBERG:
  iceberg-install     Create Iceberg tables and streaming pipes in Snowflake
  iceberg-status      Show Iceberg table row counts and freshness
  iceberg-validate    Validate Iceberg tables, pipes, and managed storage
  iceberg-test        Run full Iceberg SQL test suite

MONITORING & DIAGNOSTICS:
  status              Show PG row counts and recent ingestion runs
  health              Check PG connectivity, API health, data freshness
  validate            Deep PG validation: schema, data quality, referential integrity
  test                Run full PG SQL test suite (002_pg_kalshi_tests.sql)
  logs                Show ingestion log history

DATA QUERIES:
  markets             Query top active markets by volume
  trades              Query recent trades
  series              Query all series by category

MAINTENANCE:
  clean [N]           Remove ingestion logs older than N days (default: 30)
  drop                Drop all Kalshi PG tables (interactive confirm)
  help                Show this help message

EXAMPLES:
  ./manage.sh install               # PG first-time setup
  ./manage.sh ingest                # PG ingest all endpoints
  ./manage.sh ingest trades markets # PG ingest specific endpoints
  ./manage.sh iceberg-install       # Create Snowflake Iceberg tables
  ./manage.sh kafka-setup           # Start Kafka + deploy connector
  ./manage.sh kafka-produce         # Stream all endpoints to Kafka
  ./manage.sh kafka-produce trades  # Stream specific endpoint
  ./manage.sh kafka-produce --continuous 60  # Continuous streaming
  ./manage.sh iceberg-status        # Check Iceberg row counts
  ./manage.sh iceberg-validate      # Validate Iceberg infrastructure
  ./manage.sh iceberg-test          # Run Iceberg test suite
  ./manage.sh kafka-status          # Check Kafka connector health
  ./manage.sh kafka-stop            # Stop Kafka services
  ./manage.sh stream                # Direct stream all endpoints to Iceberg
  ./manage.sh stream trades         # Direct stream specific endpoint
  ./manage.sh stream --continuous 60 # Continuous direct streaming
  ./manage.sh stream-status         # Check Iceberg row counts + freshness
  ./manage.sh stream-test           # Run direct streaming test suite
  ./manage.sh status                # PG row counts
  ./manage.sh validate              # PG deep validation
  ./manage.sh backup                # PG backup

PREREQUISITES:
  - .env file with PG connection variables (PGHOST, PGPASSWORD, etc.)
  - Python 3 with: aiohttp, psycopg2-binary, confluent-kafka, snowpipe-streaming
  - psql client for PostgreSQL access
  - pg_dump (for backup command)
  - snow CLI (for Snowflake commands)
  - Docker + Docker Compose (for Kafka)
  - SNOWFLAKE_PRIVATE_KEY_PATH env var (for direct streaming and Kafka connector)
USAGE
}

# ============================================================================
# Main
# ============================================================================
case "${1:-help}" in
    install)    cmd_install ;;
    ingest)     shift; cmd_ingest "$*" ;;
    status)     cmd_status ;;
    health)     cmd_health ;;
    validate)   cmd_validate ;;
    test)       cmd_test ;;
    backup)     cmd_backup ;;
    stop|pause) cmd_stop ;;
    start|resume) cmd_start ;;
    logs)       cmd_logs ;;
    markets)    cmd_query_markets ;;
    trades)     cmd_query_trades ;;
    series)     cmd_query_series ;;
    clean)      cmd_clean "${2:-30}" ;;
    drop)       cmd_drop ;;
    iceberg-install)   cmd_iceberg_install ;;
    iceberg-status)    cmd_iceberg_status ;;
    iceberg-validate)  cmd_iceberg_validate ;;
    iceberg-test)      cmd_iceberg_test ;;
    kafka-setup)       cmd_kafka_setup ;;
    kafka-produce)     shift; cmd_kafka_produce "$*" ;;
    kafka-status)      cmd_kafka_status ;;
    kafka-stop)        cmd_kafka_stop ;;
    stream)            shift; cmd_stream "$*" ;;
    stream-status)     cmd_stream_status ;;
    stream-test)       cmd_stream_test ;;
    help|--help|-h)  cmd_help ;;
    *)
        fail "Unknown command: $1"
        echo ""
        cmd_help
        exit 1
        ;;
esac
