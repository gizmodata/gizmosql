#!/usr/bin/env bash
# ============================================================================
# GizmoSQL Client Shell Integration Tests
#
# Tests the gizmosql_client executable against a running gizmosql_server,
# validating real-world usage patterns: heredoc, pipe, -c flag, output modes.
#
# Usage:
#   cd build && bash ../tests/test_client_shell.sh
#
# Prerequisites:
#   - gizmosql_server and gizmosql_client must be built in the current directory
# ============================================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

PASS_COUNT=0
FAIL_COUNT=0
TEST_COUNT=0

# Server configuration
SERVER_PORT=31410
HEALTH_PORT=31411
USERNAME="shelltester"
PASSWORD="shellpass123"
DB_FILE="shell_test_client.db"
export GIZMOSQL_PASSWORD="$PASSWORD"

# Paths - assume we're running from the build directory
SERVER_BIN="./gizmosql_server"
CLIENT_BIN="./gizmosql_client"

# Cleanup function
cleanup() {
    if [ -n "${SERVER_PID:-}" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -f "$DB_FILE" "${DB_FILE}.wal"
    rm -f /tmp/gizmosql_test_output_*
}

trap cleanup EXIT

# Helper: run a test and check result
run_test() {
    local test_name="$1"
    local expected="$2"
    local actual="$3"

    TEST_COUNT=$((TEST_COUNT + 1))

    if echo "$actual" | grep -qF -- "$expected"; then
        echo -e "  ${GREEN}PASS${NC}: $test_name"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        echo -e "  ${RED}FAIL${NC}: $test_name"
        echo "    Expected to contain: $expected"
        echo "    Actual output: $(echo "$actual" | head -5)"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
}

# Helper: run a test where output should NOT contain a string
run_test_not_contains() {
    local test_name="$1"
    local not_expected="$2"
    local actual="$3"

    TEST_COUNT=$((TEST_COUNT + 1))

    if echo "$actual" | grep -qF -- "$not_expected"; then
        echo -e "  ${RED}FAIL${NC}: $test_name"
        echo "    Should NOT contain: $not_expected"
        echo "    Actual output: $(echo "$actual" | head -5)"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    else
        echo -e "  ${GREEN}PASS${NC}: $test_name"
        PASS_COUNT=$((PASS_COUNT + 1))
    fi
}

# Helper: run a test checking exit code
run_test_exit_code() {
    local test_name="$1"
    local expected_code="$2"
    local actual_code="$3"

    TEST_COUNT=$((TEST_COUNT + 1))

    if [ "$actual_code" -eq "$expected_code" ]; then
        echo -e "  ${GREEN}PASS${NC}: $test_name"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        echo -e "  ${RED}FAIL${NC}: $test_name"
        echo "    Expected exit code: $expected_code, got: $actual_code"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
}

# ============================================================================
echo "============================================================"
echo "GizmoSQL Client Shell Integration Tests"
echo "============================================================"
echo ""

# Check that binaries exist
if [ ! -x "$SERVER_BIN" ]; then
    echo -e "${RED}Error: $SERVER_BIN not found. Build with: ninja gizmosql_server${NC}"
    exit 1
fi
if [ ! -x "$CLIENT_BIN" ]; then
    echo -e "${RED}Error: $CLIENT_BIN not found. Build with: ninja gizmosql_client${NC}"
    exit 1
fi

# ============================================================================
echo "Starting GizmoSQL server on port $SERVER_PORT..."
# ============================================================================

rm -f "$DB_FILE" "${DB_FILE}.wal"

# Start server (GIZMOSQL_LICENSE_KEY_FILE can be set externally for enterprise features)
$SERVER_BIN \
    --backend duckdb \
    --database-filename "$DB_FILE" \
    --hostname localhost \
    --port "$SERVER_PORT" \
    --health-port "$HEALTH_PORT" \
    --username "$USERNAME" \
    --password "$PASSWORD" \
    > /tmp/gizmosql_test_server.log 2>&1 &

SERVER_PID=$!

# Wait for server to be ready
echo "Waiting for server to start (PID: $SERVER_PID)..."
for i in $(seq 1 30); do
    if $CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q -c "SELECT 1" > /dev/null 2>&1; then
        echo -e "${GREEN}Server is ready!${NC}"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo -e "${RED}Server failed to start within 30 seconds${NC}"
        exit 1
    fi
    sleep 1
done
echo ""

# Create test tables
$CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q -c "
CREATE TABLE IF NOT EXISTS employees (id INTEGER, name VARCHAR, dept VARCHAR, salary DOUBLE);
INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 120000);
INSERT INTO employees VALUES (2, 'Bob', 'Marketing', 85000);
INSERT INTO employees VALUES (3, 'Charlie', 'Engineering', 110000);
INSERT INTO employees VALUES (4, 'Diana', 'Marketing', 95000);
INSERT INTO employees VALUES (5, 'Eve', 'Engineering', 130000);
" > /dev/null 2>&1

# ============================================================================
echo "--- Test Group 1: Basic -c flag queries ---"
# ============================================================================

# Simple SELECT with -c
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q -c "SELECT 42 AS answer" 2>/dev/null)
run_test "Simple SELECT 42" "42" "$OUTPUT"
run_test "Column name in output" "answer" "$OUTPUT"

# Multi-row SELECT with -c
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q -c "SELECT id, name FROM employees ORDER BY id LIMIT 3" 2>/dev/null)
run_test "Multi-row query - Alice" "Alice" "$OUTPUT"
run_test "Multi-row query - Bob" "Bob" "$OUTPUT"
run_test "Multi-row query - Charlie" "Charlie" "$OUTPUT"

# Aggregation
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q -c "SELECT COUNT(*) AS cnt FROM employees" 2>/dev/null)
run_test "Aggregation COUNT" "5" "$OUTPUT"

# NULL handling
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q -c "SELECT NULL AS empty_val" 2>/dev/null)
run_test "NULL displayed" "NULL" "$OUTPUT"

echo ""

# ============================================================================
echo "--- Test Group 2: Output Modes ---"
# ============================================================================

# CSV mode
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q --csv -c "SELECT 1 AS a, 2 AS b" 2>/dev/null)
run_test "CSV mode - header" "a,b" "$OUTPUT"
run_test "CSV mode - data" "1,2" "$OUTPUT"

# JSON mode
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q --json -c "SELECT 1 AS x, 'hello' AS y" 2>/dev/null)
run_test "JSON mode - array start" "[" "$OUTPUT"
run_test "JSON mode - key x" '"x"' "$OUTPUT"
run_test "JSON mode - value hello" '"hello"' "$OUTPUT"

# Markdown mode
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q --markdown -c "SELECT 1 AS col1, 2 AS col2" 2>/dev/null)
run_test "Markdown mode - header pipe" "| col1" "$OUTPUT"
run_test "Markdown mode - separator" "| ---" "$OUTPUT"

# Table mode (ASCII)
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q --table -c "SELECT 1 AS a" 2>/dev/null)
run_test "Table mode - ASCII border" "+" "$OUTPUT"

# Box mode (default)
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q --box -c "SELECT 1 AS a" 2>/dev/null)
# Box uses unicode characters
run_test "Box mode - column name" "a" "$OUTPUT"

# No header
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q --csv --no-header -c "SELECT 1 AS a, 2 AS b" 2>/dev/null)
run_test_not_contains "No header mode" "a,b" "$OUTPUT"
run_test "No header data still present" "1,2" "$OUTPUT"

echo ""

# ============================================================================
echo "--- Test Group 3: Heredoc / Pipe Input ---"
# ============================================================================

# Single statement via heredoc
OUTPUT=$(echo "SELECT 'heredoc_test' AS val;" | $CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q 2>/dev/null)
run_test "Heredoc single statement" "heredoc_test" "$OUTPUT"

# Multiple statements via heredoc
OUTPUT=$(echo "SELECT 1 AS first; SELECT 2 AS second;" | $CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q 2>/dev/null)
run_test "Heredoc multi-statement - first" "first" "$OUTPUT"
run_test "Heredoc multi-statement - second" "second" "$OUTPUT"

# Multi-line SQL via heredoc
OUTPUT=$(cat <<'HEREDOC' | $CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q 2>/dev/null
SELECT
  id,
  name,
  dept
FROM employees
WHERE dept = 'Engineering'
ORDER BY id;
HEREDOC
)
run_test "Multi-line heredoc - Alice" "Alice" "$OUTPUT"
run_test "Multi-line heredoc - Charlie" "Charlie" "$OUTPUT"
run_test "Multi-line heredoc - Eve" "Eve" "$OUTPUT"
run_test_not_contains "Multi-line heredoc - no Bob (Marketing)" "Bob" "$OUTPUT"

# DML via pipe
OUTPUT=$(echo "INSERT INTO employees VALUES (6, 'Frank', 'Sales', 75000);" | $CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q 2>/dev/null)
run_test "INSERT via pipe" "OK" "$OUTPUT"

# Verify the insert
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q -c "SELECT name FROM employees WHERE id = 6" 2>/dev/null)
run_test "Verify INSERT result" "Frank" "$OUTPUT"

# Mixed DML and queries
OUTPUT=$(cat <<'HEREDOC' | $CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q --csv 2>/dev/null
CREATE TABLE temp_test (x INTEGER);
INSERT INTO temp_test VALUES (100);
INSERT INTO temp_test VALUES (200);
SELECT * FROM temp_test ORDER BY x;
DROP TABLE temp_test;
HEREDOC
)
run_test "Mixed DML/query - 100" "100" "$OUTPUT"
run_test "Mixed DML/query - 200" "200" "$OUTPUT"

echo ""

# ============================================================================
echo "--- Test Group 4: Environment Variables ---"
# ============================================================================

OUTPUT=$(GIZMOSQL_HOST=localhost GIZMOSQL_PORT=$SERVER_PORT GIZMOSQL_USER=$USERNAME GIZMOSQL_PASSWORD=$PASSWORD $CLIENT_BIN -q -c "SELECT 'env_test' AS result" 2>/dev/null)
run_test "Env var connection" "env_test" "$OUTPUT"

echo ""

# ============================================================================
echo "--- Test Group 5: Version and Help ---"
# ============================================================================

OUTPUT=$($CLIENT_BIN --version 2>&1)
run_test "Version output" "GizmoSQL Client" "$OUTPUT"

OUTPUT=$($CLIENT_BIN --help 2>&1)
run_test "Help - host option" "--host" "$OUTPUT"
run_test "Help - port option" "--port" "$OUTPUT"
run_test "Help - username option" "--username" "$OUTPUT"
run_test "Help - csv option" "--csv" "$OUTPUT"
run_test "Help - json option" "--json" "$OUTPUT"

echo ""

# ============================================================================
echo "--- Test Group 6: Error Handling ---"
# ============================================================================

# Invalid SQL
set +e
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q -c "SELECT * FROM nonexistent_table_xyz" 2>&1)
EXIT_CODE=$?
set -e
run_test "Invalid SQL - error message" "Error" "$OUTPUT"
run_test_exit_code "Invalid SQL - non-zero exit" 1 "$EXIT_CODE"

# Wrong password
set +e
OUTPUT=$(GIZMOSQL_PASSWORD="wrongpassword" $CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q -c "SELECT 1" 2>&1)
EXIT_CODE=$?
set -e
run_test_exit_code "Wrong password - non-zero exit" 1 "$EXIT_CODE"
run_test "Wrong password - error message" "Error" "$OUTPUT"

echo ""

# ============================================================================
echo "--- Test Group 7: File Input (-f flag) ---"
# ============================================================================

TMPFILE=$(mktemp /tmp/gizmosql_test_output_XXXXXX.sql)
cat > "$TMPFILE" <<'EOF'
SELECT 'file_test' AS source;
SELECT 1 + 2 AS sum;
EOF

OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q -f "$TMPFILE" 2>/dev/null)
run_test "File input - source" "file_test" "$OUTPUT"
run_test "File input - sum" "3" "$OUTPUT"
rm -f "$TMPFILE"

echo ""

# ============================================================================
echo "--- Test Group 8: Output to File (-o flag) ---"
# ============================================================================

OUTFILE=$(mktemp /tmp/gizmosql_test_output_XXXXXX.txt)
$CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q --csv -o "$OUTFILE" -c "SELECT 'output_test' AS val" 2>/dev/null
OUTPUT=$(cat "$OUTFILE")
run_test "Output to file" "output_test" "$OUTPUT"
rm -f "$OUTFILE"

echo ""

# ============================================================================
echo "--- Test Group 9: Complex Queries ---"
# ============================================================================

# Subquery
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q --csv -c "SELECT name FROM employees WHERE salary > (SELECT AVG(salary) FROM employees) ORDER BY name" 2>/dev/null)
run_test "Subquery - Alice high salary" "Alice" "$OUTPUT"
run_test "Subquery - Eve high salary" "Eve" "$OUTPUT"

# GROUP BY with aggregation
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q --csv -c "SELECT dept, COUNT(*) AS cnt FROM employees GROUP BY dept ORDER BY dept" 2>/dev/null)
run_test "GROUP BY - Engineering" "Engineering" "$OUTPUT"
run_test "GROUP BY - Marketing" "Marketing" "$OUTPUT"

# CASE expression
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q --csv -c "SELECT name, CASE WHEN salary > 100000 THEN 'high' ELSE 'normal' END AS tier FROM employees WHERE id = 1" 2>/dev/null)
run_test "CASE expression" "high" "$OUTPUT"

echo ""

# ============================================================================
echo "--- Test Group 10: Disconnected Mode & .connect ---"
# ============================================================================

# SQL without connection should show "not connected" error
set +e
OUTPUT=$(echo "SELECT 1;" | GIZMOSQL_PASSWORD="" GIZMOSQL_HOST="" GIZMOSQL_USER="" GIZMOSQL_PORT="" $CLIENT_BIN -q 2>&1)
set -e
run_test "SQL without connection - error" "not connected" "$OUTPUT"

# .tables without connection should show "not connected" error
set +e
OUTPUT=$(echo ".tables" | GIZMOSQL_PASSWORD="" GIZMOSQL_HOST="" GIZMOSQL_USER="" GIZMOSQL_PORT="" $CLIENT_BIN -q 2>&1)
set -e
run_test ".tables without connection - error" "not connected" "$OUTPUT"

# .connect then query via pipe
set +e
OUTPUT=$(printf ".connect localhost %s %s %s\nSELECT 'connect_test' AS val;" "$SERVER_PORT" "$USERNAME" "$PASSWORD" | GIZMOSQL_PASSWORD="" GIZMOSQL_HOST="" GIZMOSQL_USER="" GIZMOSQL_PORT="" $CLIENT_BIN -q 2>&1)
set -e
run_test ".connect then query" "connect_test" "$OUTPUT"
run_test ".connect success message" "Connected to localhost" "$OUTPUT"

# URI connection from launch
OUTPUT=$(GIZMOSQL_PASSWORD="$PASSWORD" $CLIENT_BIN "gizmosql://localhost:${SERVER_PORT}?username=${USERNAME}" -q -c "SELECT 'uri_test' AS val" 2>/dev/null)
run_test "URI launch connection" "uri_test" "$OUTPUT"

# URI connection with --uri flag
OUTPUT=$(GIZMOSQL_PASSWORD="$PASSWORD" $CLIENT_BIN --uri "gizmosql://localhost:${SERVER_PORT}?username=${USERNAME}" -q -c "SELECT 'uri_flag_test' AS val" 2>/dev/null)
run_test "URI flag connection" "uri_flag_test" "$OUTPUT"

echo ""

# ============================================================================
echo "--- Test Group 11: Row/Column Truncation ---"
# ============================================================================

# Default box mode with large result set (non-interactive = no truncation)
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q --box -c "SELECT x FROM generate_series(1, 100) t(x)" 2>/dev/null)
run_test "Non-interactive shows all rows - 100 rows" "100 rows" "$OUTPUT"

# Type row in box mode
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q --box -c "SELECT 42 AS num, 'hello' AS str" 2>/dev/null)
run_test "Box mode shows integer type" "int" "$OUTPUT"
run_test "Box mode shows string type" "string" "$OUTPUT"

# Type row in table mode
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q --table -c "SELECT 42 AS num, 'hello' AS str" 2>/dev/null)
run_test "Table mode shows integer type" "int" "$OUTPUT"
run_test "Table mode shows string type" "string" "$OUTPUT"

# CSV mode does NOT show type row (regression test)
OUTPUT=$($CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q --csv -c "SELECT 42 AS num" 2>/dev/null)
run_test_not_contains "CSV mode no type row" "int" "$OUTPUT"

# Dot commands via pipe
OUTPUT=$(printf ".maxrows 5\nSELECT x FROM generate_series(1, 20) t(x);" | $CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q 2>/dev/null)
run_test "maxrows truncation footer" "5 shown" "$OUTPUT"
run_test "maxrows total row count" "20 rows" "$OUTPUT"

# .maxrows 0 disables truncation via pipe
OUTPUT=$(printf ".maxrows 0\nSELECT x FROM generate_series(1, 20) t(x);" | $CLIENT_BIN -h localhost -p "$SERVER_PORT" -u "$USERNAME" -q 2>/dev/null)
run_test "maxrows 0 shows all rows" "20 rows" "$OUTPUT"
run_test_not_contains "maxrows 0 no truncation indicator" "shown" "$OUTPUT"

echo ""

# ============================================================================
# Summary
# ============================================================================
echo "============================================================"
echo "Test Results: $PASS_COUNT passed, $FAIL_COUNT failed (out of $TEST_COUNT)"
echo "============================================================"

if [ "$FAIL_COUNT" -gt 0 ]; then
    echo -e "${RED}SOME TESTS FAILED${NC}"
    exit 1
else
    echo -e "${GREEN}ALL TESTS PASSED${NC}"
    exit 0
fi
