#!/bin/bash
# Test script for OpenTelemetry integration with Jaeger
# Usage: ./scripts/test_telemetry.sh [build_dir]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="${1:-$PROJECT_DIR/build}"

JAEGER_CONTAINER="gizmosql-jaeger-test"
GIZMOSQL_PORT=31337
JAEGER_OTLP_PORT=4317
JAEGER_UI_PORT=16686
TEST_PASSWORD="test_password_123"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

cleanup() {
    log_info "Cleaning up..."

    # Stop GizmoSQL server if running
    if [ -n "$GIZMOSQL_PID" ] && kill -0 "$GIZMOSQL_PID" 2>/dev/null; then
        log_info "Stopping GizmoSQL server (PID: $GIZMOSQL_PID)..."
        kill "$GIZMOSQL_PID" 2>/dev/null || true
        wait "$GIZMOSQL_PID" 2>/dev/null || true
    fi

    # Stop Jaeger container
    if docker ps -q -f name="$JAEGER_CONTAINER" | grep -q .; then
        log_info "Stopping Jaeger container..."
        docker stop "$JAEGER_CONTAINER" >/dev/null 2>&1 || true
    fi

    # Remove Jaeger container
    if docker ps -aq -f name="$JAEGER_CONTAINER" | grep -q .; then
        docker rm "$JAEGER_CONTAINER" >/dev/null 2>&1 || true
    fi

    # Remove test database
    rm -f /tmp/gizmosql_telemetry_test.db
}

trap cleanup EXIT

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed. Please install Docker first."
        exit 1
    fi

    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running. Please start Docker."
        exit 1
    fi

    if [ ! -f "$BUILD_DIR/gizmosql_server" ]; then
        log_error "GizmoSQL server not found at $BUILD_DIR/gizmosql_server"
        log_error "Please build the project first: cmake --build $BUILD_DIR"
        exit 1
    fi

    if [ ! -f "$BUILD_DIR/gizmosql_client" ]; then
        log_error "GizmoSQL client not found at $BUILD_DIR/gizmosql_client"
        exit 1
    fi

    log_info "All prerequisites met."
}

# Start Jaeger
start_jaeger() {
    log_info "Starting Jaeger container..."

    # Remove existing container if present
    docker rm -f "$JAEGER_CONTAINER" 2>/dev/null || true

    docker run -d \
        --name "$JAEGER_CONTAINER" \
        -p "$JAEGER_OTLP_PORT:4317" \
        -p "$JAEGER_UI_PORT:16686" \
        -e COLLECTOR_OTLP_ENABLED=true \
        jaegertracing/all-in-one:latest

    # Wait for Jaeger to be ready
    log_info "Waiting for Jaeger to be ready..."
    for i in {1..30}; do
        if curl -s "http://localhost:$JAEGER_UI_PORT" > /dev/null 2>&1; then
            log_info "Jaeger is ready!"
            return 0
        fi
        sleep 1
    done

    log_error "Jaeger failed to start within 30 seconds"
    exit 1
}

# Start GizmoSQL server with telemetry
start_gizmosql() {
    log_info "Starting GizmoSQL server with OpenTelemetry enabled..."

    "$BUILD_DIR/gizmosql_server" \
        --backend duckdb \
        --database-filename /tmp/gizmosql_telemetry_test.db \
        --port "$GIZMOSQL_PORT" \
        --password "$TEST_PASSWORD" \
        --otel-enabled on \
        --otel-exporter grpc \
        --otel-endpoint "localhost:$JAEGER_OTLP_PORT" \
        --otel-service-name "gizmosql-telemetry-test" \
        --log-level info \
        --access-log on &

    GIZMOSQL_PID=$!

    # Wait for server to be ready
    log_info "Waiting for GizmoSQL server to be ready..."
    for i in {1..30}; do
        if nc -z localhost "$GIZMOSQL_PORT" 2>/dev/null; then
            log_info "GizmoSQL server is ready! (PID: $GIZMOSQL_PID)"
            return 0
        fi
        sleep 1
    done

    log_error "GizmoSQL server failed to start within 30 seconds"
    exit 1
}

# Run test queries
run_test_queries() {
    log_info "Running test queries to generate telemetry data..."

    # Use the GizmoSQL client to run queries
    # Note: Adjust these commands based on your client's interface

    log_info "Creating test table..."
    "$BUILD_DIR/gizmosql_client" \
        --host localhost \
        --port "$GIZMOSQL_PORT" \
        --username gizmosql_username \
        --password "$TEST_PASSWORD" \
        --query "CREATE TABLE IF NOT EXISTS telemetry_test (id INTEGER, name VARCHAR, value DOUBLE)" \
        2>/dev/null || log_warn "Table creation may have failed (might already exist)"

    log_info "Inserting test data..."
    for i in {1..5}; do
        "$BUILD_DIR/gizmosql_client" \
            --host localhost \
            --port "$GIZMOSQL_PORT" \
            --username gizmosql_username \
            --password "$TEST_PASSWORD" \
            --query "INSERT INTO telemetry_test VALUES ($i, 'test_$i', $i.5)" \
            2>/dev/null || true
    done

    log_info "Running SELECT queries..."
    for i in {1..3}; do
        "$BUILD_DIR/gizmosql_client" \
            --host localhost \
            --port "$GIZMOSQL_PORT" \
            --username gizmosql_username \
            --password "$TEST_PASSWORD" \
            --query "SELECT * FROM telemetry_test WHERE id > 0" \
            2>/dev/null || true
    done

    log_info "Test queries completed."
}

# Verify traces in Jaeger
verify_traces() {
    log_info "Waiting for traces to be exported (5 seconds)..."
    sleep 5

    log_info "Checking for traces in Jaeger..."

    # Query Jaeger API for traces from our service
    TRACES=$(curl -s "http://localhost:$JAEGER_UI_PORT/api/traces?service=gizmosql-telemetry-test&limit=10")

    if echo "$TRACES" | grep -q '"traceID"'; then
        TRACE_COUNT=$(echo "$TRACES" | grep -o '"traceID"' | wc -l)
        log_info "Found $TRACE_COUNT traces in Jaeger!"
        log_info ""
        log_info "========================================"
        log_info "  TELEMETRY TEST PASSED!"
        log_info "========================================"
        log_info ""
        log_info "View traces at: http://localhost:$JAEGER_UI_PORT"
        log_info "Service name: gizmosql-telemetry-test"
        log_info ""
        return 0
    else
        log_warn "No traces found yet. This could mean:"
        log_warn "  1. Traces haven't been exported yet (try waiting longer)"
        log_warn "  2. There's a connection issue with the OTLP endpoint"
        log_warn ""
        log_info "You can manually check Jaeger at: http://localhost:$JAEGER_UI_PORT"
        log_info "Look for service: gizmosql-telemetry-test"
        return 1
    fi
}

# Main
main() {
    echo ""
    echo "============================================"
    echo "  GizmoSQL OpenTelemetry Integration Test"
    echo "============================================"
    echo ""

    check_prerequisites
    start_jaeger
    start_gizmosql
    run_test_queries
    verify_traces

    echo ""
    log_info "Press Ctrl+C to stop the test and cleanup..."
    log_info "Jaeger UI available at: http://localhost:$JAEGER_UI_PORT"
    echo ""

    # Keep running until interrupted
    wait "$GIZMOSQL_PID"
}

main "$@"
