# Database Examples

## Quick Start

Start the database server:
```bash
cargo run
```

The server runs on `0.0.0.0:1231` by default.

## SQL Examples

### Create a Table
```bash
curl -X POST http://localhost:1231/sql \
  -d "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))"
```

### Insert Data
```bash
curl -X POST http://localhost:1231/sql \
  -d "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')"

curl -X POST http://localhost:1231/sql \
  -d "INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')"
```

### Query Data
```bash
# Select all
curl -X POST http://localhost:1231/sql \
  -d "SELECT * FROM users"

# Select with WHERE
curl -X POST http://localhost:1231/sql \
  -d "SELECT name, email FROM users WHERE id = 1"
```

### Update Data
```bash
curl -X POST http://localhost:1231/sql \
  -d "UPDATE users SET email = 'alice.new@example.com' WHERE id = 1"
```

### Delete Data
```bash
curl -X POST http://localhost:1231/sql \
  -d "DELETE FROM users WHERE id = 2"
```

### List Tables
```bash
curl http://localhost:1231/tables
```

## Data Persistence

Data is stored using B+ trees in `~/.butterfly_db/data/`:
- Each table has its own `.db` file
- Binary format with bincode serialization
- Data persists across server restarts

## Performance Benchmark

Compare butterfly_db against SQLite3:

```bash
# Run with default 100 records
./examples/benchmark.sh

# Run with custom record count
./examples/benchmark.sh 500
```

Sample output:
```
========================================
  RESULTS (100 records)
========================================

Operation                    butterfly_db         SQLite3
------------------------- --------------- ---------------
INSERT (100 rows)                0.980s        0.580s
INSERT rate                        102/s          172/s
SELECT * (all rows)              0.010s        0.003s
SELECT WHERE                     0.009s        0.004s

=== Analysis ===
INSERT: SQLite is 1.69x faster
SELECT: SQLite is 3.33x faster

Note: butterfly_db uses HTTP which adds network overhead.
SQLite uses direct file I/O which is faster for local ops.
```

## Configuration

Edit `config.toml` to configure:

```toml
[pool]
min_connections = 5
max_connections = 100
connection_timeout_ms = 5000
idle_timeout_ms = 60000
```
