#!/bin/bash
# Comprehensive Benchmark: butterfly_db vs SQLite3
# Measures INSERT, SELECT, UPDATE, DELETE, Aggregate, and JOIN performance

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
BUTTERFLY_URL="http://localhost:1231"
SQLITE_DB="/tmp/benchmark_test.db"
NUM_RECORDS=${1:-10}  # Default 1000 records

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     Database Benchmark: butterfly_db vs SQLite3            ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "Records: ${YELLOW}$NUM_RECORDS${NC}"
echo ""

# 9241039170

# Check if butterfly_db is running
if ! curl -s "$BUTTERFLY_URL/ping" > /dev/null 2>&1; then
    echo -e "${RED}Error: butterfly_db is not running on $BUTTERFLY_URL${NC}"
    echo "Please start it with: cargo run --release"
    exit 1
fi

# Clean up
rm -f "$SQLITE_DB"
curl -s -X POST "$BUTTERFLY_URL/sql" -d "DROP TABLE IF EXISTS benchmark" > /dev/null 2>&1 || true
curl -s -X POST "$BUTTERFLY_URL/sql" -d "DROP TABLE IF EXISTS benchmark2" > /dev/null 2>&1 || true

echo -e "${GREEN}=== Setting up tables ===${NC}"

# Create tables
curl -s -X POST "$BUTTERFLY_URL/sql" \
    -d "CREATE TABLE benchmark (id INTEGER PRIMARY KEY, name VARCHAR(255), value INTEGER)" > /dev/null
curl -s -X POST "$BUTTERFLY_URL/sql" \
    -d "CREATE TABLE benchmark2 (id INTEGER PRIMARY KEY, bench_id INTEGER, score INTEGER)" > /dev/null

sqlite3 "$SQLITE_DB" "CREATE TABLE benchmark (id INTEGER PRIMARY KEY, name TEXT, value INTEGER);"
sqlite3 "$SQLITE_DB" "CREATE TABLE benchmark2 (id INTEGER PRIMARY KEY, bench_id INTEGER, score INTEGER);"

# ==========================================
#             INSERT Benchmark
# ==========================================
echo ""
echo -e "${CYAN}━━━ INSERT Benchmark ($NUM_RECORDS records) ━━━${NC}"

BUTTERFLY_INSERT_START=$(date +%s.%N)
for i in $(seq 1 $NUM_RECORDS); do
    curl -s -X POST "$BUTTERFLY_URL/sql" \
        -d "INSERT INTO benchmark (id, name, value) VALUES ($i, 'user_$i', $((i * 10)))" > /dev/null
done
BUTTERFLY_INSERT_END=$(date +%s.%N)
BUTTERFLY_INSERT_TIME=$(echo "$BUTTERFLY_INSERT_END - $BUTTERFLY_INSERT_START" | bc)

SQLITE_INSERT_START=$(date +%s.%N)
for i in $(seq 1 $NUM_RECORDS); do
    sqlite3 "$SQLITE_DB" "INSERT INTO benchmark (id, name, value) VALUES ($i, 'user_$i', $((i * 10)));"
done
SQLITE_INSERT_END=$(date +%s.%N)
SQLITE_INSERT_TIME=$(echo "$SQLITE_INSERT_END - $SQLITE_INSERT_START" | bc)

echo -e "  butterfly_db: ${BUTTERFLY_INSERT_TIME}s"
echo -e "  SQLite:       ${SQLITE_INSERT_TIME}s"

# Insert into second table for JOINs
for i in $(seq 1 $((NUM_RECORDS / 10))); do
    curl -s -X POST "$BUTTERFLY_URL/sql" \
        -d "INSERT INTO benchmark2 (id, bench_id, score) VALUES ($i, $((i * 10)), $((RANDOM % 100)))" > /dev/null
    sqlite3 "$SQLITE_DB" "INSERT INTO benchmark2 (id, bench_id, score) VALUES ($i, $((i * 10)), $((RANDOM % 100)));"
done

# ==========================================
# SELECT Benchmark
# ==========================================
echo ""
echo -e "${CYAN}━━━ SELECT Benchmark ━━━${NC}"

# SELECT ALL
BUTTERFLY_SELECT_START=$(date +%s.%N)
curl -s -X POST "$BUTTERFLY_URL/sql" -d "SELECT * FROM benchmark" > /dev/null
BUTTERFLY_SELECT_END=$(date +%s.%N)
BUTTERFLY_SELECT_TIME=$(echo "$BUTTERFLY_SELECT_END - $BUTTERFLY_SELECT_START" | bc)

SQLITE_SELECT_START=$(date +%s.%N)
sqlite3 "$SQLITE_DB" "SELECT * FROM benchmark;" > /dev/null
SQLITE_SELECT_END=$(date +%s.%N)
SQLITE_SELECT_TIME=$(echo "$SQLITE_SELECT_END - $SQLITE_SELECT_START" | bc)

echo -e "  SELECT *:     butterfly_db: ${BUTTERFLY_SELECT_TIME}s, SQLite: ${SQLITE_SELECT_TIME}s"

# SELECT WHERE
BUTTERFLY_WHERE_START=$(date +%s.%N)
curl -s -X POST "$BUTTERFLY_URL/sql" -d "SELECT * FROM benchmark WHERE value > 5000" > /dev/null
BUTTERFLY_WHERE_END=$(date +%s.%N)
BUTTERFLY_WHERE_TIME=$(echo "$BUTTERFLY_WHERE_END - $BUTTERFLY_WHERE_START" | bc)

SQLITE_WHERE_START=$(date +%s.%N)
sqlite3 "$SQLITE_DB" "SELECT * FROM benchmark WHERE value > 5000;" > /dev/null
SQLITE_WHERE_END=$(date +%s.%N)
SQLITE_WHERE_TIME=$(echo "$SQLITE_WHERE_END - $SQLITE_WHERE_START" | bc)

echo -e "  SELECT WHERE: butterfly_db: ${BUTTERFLY_WHERE_TIME}s, SQLite: ${SQLITE_WHERE_TIME}s"

# ==========================================
# Aggregate Benchmark
# ==========================================
echo ""
echo -e "${CYAN}━━━ Aggregate Benchmark ━━━${NC}"

BUTTERFLY_AGG_START=$(date +%s.%N)
curl -s -X POST "$BUTTERFLY_URL/sql" -d "SELECT COUNT(*) FROM benchmark" > /dev/null
curl -s -X POST "$BUTTERFLY_URL/sql" -d "SELECT SUM(value) FROM benchmark" > /dev/null
curl -s -X POST "$BUTTERFLY_URL/sql" -d "SELECT AVG(value) FROM benchmark" > /dev/null
curl -s -X POST "$BUTTERFLY_URL/sql" -d "SELECT MIN(value) FROM benchmark" > /dev/null
curl -s -X POST "$BUTTERFLY_URL/sql" -d "SELECT MAX(value) FROM benchmark" > /dev/null
BUTTERFLY_AGG_END=$(date +%s.%N)
BUTTERFLY_AGG_TIME=$(echo "$BUTTERFLY_AGG_END - $BUTTERFLY_AGG_START" | bc)

SQLITE_AGG_START=$(date +%s.%N)
sqlite3 "$SQLITE_DB" "SELECT COUNT(*) FROM benchmark;" > /dev/null
sqlite3 "$SQLITE_DB" "SELECT SUM(value) FROM benchmark;" > /dev/null
sqlite3 "$SQLITE_DB" "SELECT AVG(value) FROM benchmark;" > /dev/null
sqlite3 "$SQLITE_DB" "SELECT MIN(value) FROM benchmark;" > /dev/null
sqlite3 "$SQLITE_DB" "SELECT MAX(value) FROM benchmark;" > /dev/null
SQLITE_AGG_END=$(date +%s.%N)
SQLITE_AGG_TIME=$(echo "$SQLITE_AGG_END - $SQLITE_AGG_START" | bc)

echo -e "  5 aggregates: butterfly_db: ${BUTTERFLY_AGG_TIME}s, SQLite: ${SQLITE_AGG_TIME}s"

# ==========================================
# UPDATE Benchmark
# ==========================================
echo ""
echo -e "${CYAN}━━━ UPDATE Benchmark ━━━${NC}"

BUTTERFLY_UPDATE_START=$(date +%s.%N)
curl -s -X POST "$BUTTERFLY_URL/sql" -d "UPDATE benchmark SET value = value + 1 WHERE id < 100" > /dev/null
BUTTERFLY_UPDATE_END=$(date +%s.%N)
BUTTERFLY_UPDATE_TIME=$(echo "$BUTTERFLY_UPDATE_END - $BUTTERFLY_UPDATE_START" | bc)

SQLITE_UPDATE_START=$(date +%s.%N)
sqlite3 "$SQLITE_DB" "UPDATE benchmark SET value = value + 1 WHERE id < 100;"
SQLITE_UPDATE_END=$(date +%s.%N)
SQLITE_UPDATE_TIME=$(echo "$SQLITE_UPDATE_END - $SQLITE_UPDATE_START" | bc)

echo -e "  UPDATE:       butterfly_db: ${BUTTERFLY_UPDATE_TIME}s, SQLite: ${SQLITE_UPDATE_TIME}s"

# ==========================================
# DELETE Benchmark
# ==========================================
echo ""
echo -e "${CYAN}━━━ DELETE Benchmark ━━━${NC}"

BUTTERFLY_DELETE_START=$(date +%s.%N)
curl -s -X POST "$BUTTERFLY_URL/sql" -d "DELETE FROM benchmark WHERE id > $((NUM_RECORDS - 10))" > /dev/null
BUTTERFLY_DELETE_END=$(date +%s.%N)
BUTTERFLY_DELETE_TIME=$(echo "$BUTTERFLY_DELETE_END - $BUTTERFLY_DELETE_START" | bc)

SQLITE_DELETE_START=$(date +%s.%N)
sqlite3 "$SQLITE_DB" "DELETE FROM benchmark WHERE id > $((NUM_RECORDS - 10));"
SQLITE_DELETE_END=$(date +%s.%N)
SQLITE_DELETE_TIME=$(echo "$SQLITE_DELETE_END - $SQLITE_DELETE_START" | bc)

echo -e "  DELETE:       butterfly_db: ${BUTTERFLY_DELETE_TIME}s, SQLite: ${SQLITE_DELETE_TIME}s"

# ==========================================
# Results Summary
# ==========================================
echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                    RESULTS SUMMARY                         ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

BUTTERFLY_INSERT_RPS=$(echo "scale=0; $NUM_RECORDS / $BUTTERFLY_INSERT_TIME" | bc)
SQLITE_INSERT_RPS=$(echo "scale=0; $NUM_RECORDS / $SQLITE_INSERT_TIME" | bc)

printf "${YELLOW}%-25s %15s %15s${NC}\n" "Operation" "butterfly_db" "SQLite3"
printf "%-25s %15s %15s\n" "─────────────────────────" "───────────────" "───────────────"
printf "%-25s %12.3fs %12.3fs\n" "INSERT ($NUM_RECORDS rows)" "$BUTTERFLY_INSERT_TIME" "$SQLITE_INSERT_TIME"
printf "%-25s %12d/s %12d/s\n" "  └─ Rate" "$BUTTERFLY_INSERT_RPS" "$SQLITE_INSERT_RPS"
printf "%-25s %12.3fs %12.3fs\n" "SELECT *" "$BUTTERFLY_SELECT_TIME" "$SQLITE_SELECT_TIME"
printf "%-25s %12.3fs %12.3fs\n" "SELECT WHERE" "$BUTTERFLY_WHERE_TIME" "$SQLITE_WHERE_TIME"
printf "%-25s %12.3fs %12.3fs\n" "Aggregates (5x)" "$BUTTERFLY_AGG_TIME" "$SQLITE_AGG_TIME"
printf "%-25s %12.3fs %12.3fs\n" "UPDATE" "$BUTTERFLY_UPDATE_TIME" "$SQLITE_UPDATE_TIME"
printf "%-25s %12.3fs %12.3fs\n" "DELETE" "$BUTTERFLY_DELETE_TIME" "$SQLITE_DELETE_TIME"

echo ""
echo -e "${GREEN}=== Analysis ===${NC}"

INSERT_RATIO=$(echo "scale=2; $BUTTERFLY_INSERT_TIME / $SQLITE_INSERT_TIME" | bc)
SELECT_RATIO=$(echo "scale=2; $BUTTERFLY_SELECT_TIME / $SQLITE_SELECT_TIME" | bc)

echo -e "• butterfly_db uses HTTP (adds latency)"
echo -e "• SQLite uses direct file I/O"
echo -e "• butterfly_db uses B+ tree storage"

if (( $(echo "$INSERT_RATIO > 1" | bc -l) )); then
    echo -e "• INSERT: SQLite is ${YELLOW}${INSERT_RATIO}x faster${NC}"
else
    echo -e "• INSERT: butterfly_db is ${GREEN}$(echo "scale=1; 1/$INSERT_RATIO" | bc)x faster${NC}"
fi

echo ""

# Cleanup
rm -f "$SQLITE_DB"
curl -s -X POST "$BUTTERFLY_URL/sql" -d "DROP TABLE benchmark" > /dev/null 2>&1 || true
curl -s -X POST "$BUTTERFLY_URL/sql" -d "DROP TABLE benchmark2" > /dev/null 2>&1 || true

echo -e "${GREEN}Benchmark complete!${NC}"
