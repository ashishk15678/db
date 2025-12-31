#!/bin/bash
# Example SQL queries for butterfly_db

BASE_URL="http://localhost:1231"

echo "=== Creating tables ==="
curl -s -X POST "$BASE_URL/sql" -d "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))"
echo ""

curl -s -X POST "$BASE_URL/sql" -d "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title VARCHAR(255), content TEXT)"
echo ""

echo "=== Inserting data ==="
curl -s -X POST "$BASE_URL/sql" -d "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')"
echo ""

curl -s -X POST "$BASE_URL/sql" -d "INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')"
echo ""

curl -s -X POST "$BASE_URL/sql" -d "INSERT INTO posts (id, user_id, title, content) VALUES (1, 1, 'Hello World', 'My first post')"
echo ""

echo "=== Querying data ==="
curl -s -X POST "$BASE_URL/sql" -d "SELECT * FROM users"
echo ""

curl -s -X POST "$BASE_URL/sql" -d "SELECT * FROM posts"
echo ""

echo "=== Update example ==="
curl -s -X POST "$BASE_URL/sql" -d "UPDATE users SET email = 'alice.updated@example.com' WHERE id = 1"
echo ""

curl -s -X POST "$BASE_URL/sql" -d "SELECT * FROM users WHERE id = 1"
echo ""

echo "=== List tables ==="
curl -s "$BASE_URL/tables"
echo ""

echo "=== Done! ==="
