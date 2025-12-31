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

Data is automatically saved to `~/.butterfly_db/`:
- `catalog.json` - Table schemas and metadata
- `data.json` - Table row data

Data persists across server restarts.

## Configuration

Edit `config.toml` to configure:

```toml
[pool]
min_connections = 5
max_connections = 100
connection_timeout_ms = 5000
idle_timeout_ms = 60000
```
