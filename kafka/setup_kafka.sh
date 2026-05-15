#!/usr/bin/env bash
##############################################################################
# setup_kafka.sh — Build & deploy Kalshi Kafka streaming pipeline
#
# Usage:
#   ./kafka/setup_kafka.sh start       # Start all services
#   ./kafka/setup_kafka.sh topics      # Create Kafka topics
#   ./kafka/setup_kafka.sh deploy      # Deploy Snowflake connector
#   ./kafka/setup_kafka.sh status      # Check connector status
#   ./kafka/setup_kafka.sh stop        # Stop all services
#   ./kafka/setup_kafka.sh logs        # Tail connector logs
#   ./kafka/setup_kafka.sh full        # Start + topics + deploy
#   ./kafka/setup_kafka.sh help        # Show this help
##############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# --- Colors ---
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

# Kafka topics for Kalshi data
TOPICS=("kalshi-series" "kalshi-markets" "kalshi-trades" "kalshi-tags" "kalshi-sports")
CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="kalshi-snowflake-sink"

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

cmd_start() {
    header "Starting Kafka Infrastructure"

    info "Starting Docker Compose (RedPanda + Kafka Connect)..."
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d

    info "Waiting for RedPanda to be healthy..."
    local retries=30
    while [[ $retries -gt 0 ]]; do
        if docker exec kalshi-redpanda rpk cluster health &>/dev/null; then
            ok "RedPanda is healthy"
            break
        fi
        retries=$((retries - 1))
        sleep 2
    done
    if [[ $retries -eq 0 ]]; then
        fail "RedPanda did not become healthy in time"
        return 1
    fi

    info "Waiting for Kafka Connect to be ready..."
    retries=60
    while [[ $retries -gt 0 ]]; do
        if curl -sf "$CONNECT_URL/connectors" &>/dev/null; then
            ok "Kafka Connect is ready"
            break
        fi
        retries=$((retries - 1))
        sleep 3
    done
    if [[ $retries -eq 0 ]]; then
        fail "Kafka Connect did not start in time"
        return 1
    fi

    ok "All services running"
    echo ""
    info "RedPanda Console: http://localhost:8080"
    info "Kafka Connect:    http://localhost:8083"
    info "Broker:           localhost:9092 (host) / 192.168.1.172:19092 (LAN)"
}

cmd_topics() {
    header "Creating Kafka Topics"

    for topic in "${TOPICS[@]}"; do
        info "Creating topic: $topic"
        docker exec kalshi-redpanda rpk topic create "$topic" \
            --partitions 3 \
            --replicas 1 \
            2>/dev/null || true
    done

    echo ""
    info "Existing topics:"
    docker exec kalshi-redpanda rpk topic list
    ok "Topics ready"
}

cmd_deploy() {
    header "Deploying Snowflake Kafka Connector"

    # Read private key from environment or file
    local private_key=""
    if [[ -n "${SNOWFLAKE_PRIVATE_KEY:-}" ]]; then
        private_key="$SNOWFLAKE_PRIVATE_KEY"
    elif [[ -f "$PROJECT_DIR/kafka/snowflake_kafkaguy_rsa_key.p8" ]]; then
        private_key=$(grep -v "BEGIN\|END" "$PROJECT_DIR/kafka/snowflake_kafkaguy_rsa_key.p8" | tr -d '\n')
    else
        fail "No private key found."
        info "Set SNOWFLAKE_PRIVATE_KEY env var or place key at kafka/snowflake_kafkaguy_rsa_key.p8"
        return 1
    fi

    # Build connector JSON payload
    local payload
    payload=$(cat <<EOJSON
{
  "name": "${CONNECTOR_NAME}",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "tasks.max": "1",
    "topics": "kalshi-series,kalshi-markets,kalshi-trades,kalshi-tags,kalshi-sports",
    "snowflake.url.name": "${SNOWFLAKE_URL:-LXB29530.us-east-1.snowflakecomputing.com}",
    "snowflake.user.name": "KAFKAGUY",
    "snowflake.role.name": "ACCOUNTADMIN",
    "snowflake.private.key": "${private_key}",
    "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
    "snowflake.database.name": "KALSHI_DB",
    "snowflake.schema.name": "STREAMING",
    "snowflake.topic2table.map": "kalshi-series:KALSHI_SERIES,kalshi-markets:KALSHI_MARKETS,kalshi-trades:KALSHI_TRADES,kalshi-tags:KALSHI_CATEGORIES_TAGS,kalshi-sports:KALSHI_SPORTS_FILTERS",
    "snowflake.enable.schematization": "FALSE",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "com.snowflake.kafka.connector.records.SnowflakeJsonConverter",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",
    "buffer.count.records": "10000",
    "buffer.flush.time": "120",
    "buffer.size.bytes": "104857600"
  }
}
EOJSON
)

    # Check if connector already exists
    local http_code
    http_code=$(curl -s -o /dev/null -w "%{http_code}" "$CONNECT_URL/connectors/$CONNECTOR_NAME" 2>/dev/null || echo "000")
    if [[ "$http_code" == "200" ]]; then
        info "Connector already exists, updating..."
        curl -s -X PUT "$CONNECT_URL/connectors/$CONNECTOR_NAME/config" \
            -H "Content-Type: application/json" \
            -d "$(echo "$payload" | python3 -c 'import sys,json; print(json.dumps(json.load(sys.stdin)["config"]))')" | python3 -m json.tool
    else
        info "Creating new connector..."
        curl -s -X POST "$CONNECT_URL/connectors" \
            -H "Content-Type: application/json" \
            -d "$payload" | python3 -m json.tool
    fi

    sleep 3
    cmd_status
}

cmd_status() {
    header "Connector Status"

    # Connector list
    echo -e "\n${BOLD}Registered Connectors:${NC}"
    curl -s "$CONNECT_URL/connectors" 2>/dev/null | python3 -m json.tool || warn "Connect API not reachable"

    # Connector detail
    echo -e "\n${BOLD}Kalshi Connector Status:${NC}"
    curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" 2>/dev/null | python3 -m json.tool || warn "Connector not found"

    # Topic offsets
    echo -e "\n${BOLD}Topic Offsets:${NC}"
    for topic in "${TOPICS[@]}"; do
        local offsets
        offsets=$(docker exec kalshi-redpanda rpk topic consume "$topic" --num 0 --format '%e\n' 2>/dev/null | wc -l || echo "?")
        echo "  $topic: $offsets messages"
    done 2>/dev/null || warn "Could not read topic offsets"

    # Topic details
    echo -e "\n${BOLD}Topic Details:${NC}"
    docker exec kalshi-redpanda rpk topic list 2>/dev/null || warn "RedPanda not running"
}

cmd_stop() {
    header "Stopping Kafka Infrastructure"

    info "Stopping Docker Compose..."
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" down

    ok "All Kafka services stopped"
}

cmd_logs() {
    header "Kafka Connect Logs"
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" logs -f connect
}

cmd_destroy() {
    header "Destroying Kafka Infrastructure"

    warn "This will remove all containers, volumes, and topic data."
    read -rp "Are you sure? (yes/no): " confirm
    if [[ "$confirm" != "yes" ]]; then
        info "Aborted."
        return
    fi

    docker compose -f "$SCRIPT_DIR/docker-compose.yml" down -v
    ok "All Kafka infrastructure destroyed"
}

cmd_full() {
    header "Full Setup: Start + Topics + Deploy"
    cmd_start
    echo ""
    cmd_topics
    echo ""
    cmd_deploy
}

cmd_help() {
    cat <<'USAGE'
Kalshi Kafka Pipeline Setup

USAGE: ./kafka/setup_kafka.sh <command>

COMMANDS:
  start         Start RedPanda + Kafka Connect (Docker Compose)
  topics        Create Kafka topics for all Kalshi endpoints
  deploy        Deploy Snowflake Kafka Connector (requires RSA private key)
  status        Show connector status and topic offsets
  stop          Stop all Kafka services
  logs          Tail Kafka Connect logs
  destroy       Remove all containers and volumes (destructive)
  full          Complete setup: start + topics + deploy
  help          Show this help

PREREQUISITES:
  - Docker and Docker Compose
  - KAFKAGUY RSA private key (set SNOWFLAKE_PRIVATE_KEY or place at kafka/snowflake_kafkaguy_rsa_key.p8)
  - Snowflake Iceberg tables created (run: ./manage.sh iceberg-install)

BROKER ENDPOINTS:
  localhost:9092          From host machine
  192.168.1.172:19092     From LAN (RedPanda / StreamNative)
  redpanda:29092          From Docker internal network

MONITORING:
  RedPanda Console:  http://localhost:8080
  Kafka Connect API: http://localhost:8083
USAGE
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
case "${1:-help}" in
    start)      cmd_start ;;
    topics)     cmd_topics ;;
    deploy)     cmd_deploy ;;
    status)     cmd_status ;;
    stop)       cmd_stop ;;
    logs)       cmd_logs ;;
    destroy)    cmd_destroy ;;
    full)       cmd_full ;;
    help|--help|-h) cmd_help ;;
    *)
        fail "Unknown command: $1"
        echo ""
        cmd_help
        exit 1
        ;;
esac
