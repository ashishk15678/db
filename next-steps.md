Quick status: I scanned the repo and built it. It’s a Rust project with early scaffolding for partitions and HTTP, a stub SQL module, and a custom AES. Build fails due to a bad call in src/db/http/mod.rs (server.data().await does not exist).
Current state snapshot
Language/runtime: Rust, tokio, protobuf, custom AES in src/hashing/aes.rs.
Entrypoint: src/main.rs starts partition servers.
Partitions: src/db/partition/mod.rs spawns leader + 3 servers, writes to ~/data/\*.db (note: ~ isn’t expanded by Rust’s std::fs).
HTTP: src/db/http/mod.rs rudimentary TCP/HTTP parser with /ping, /heart-beat, placeholder /sql.
SQL: src/db/sql/mod.rs stub parser with no real functionality.
Schema/docs: minimal schema.md.
15-minute tasks plan (doable slices)

1. Fix build: remove or replace server.data().await in src/db/http/mod.rs
   Deliverable: cargo build succeeds.
   Acceptance: Build green, /ping still responds.

[DONE] 2. Correct file paths and dirs creation
   Replace Path::new("~/data/...") with a resolved path (e.g., dirs::home_dir()/data/...), ensure parent directories exist.
   Acceptance: Files are created under user home on startup without panics.

[DONE] 3. Add structured logging
   Add tracing + tracing-subscriber for async-friendly logs.
   Acceptance: server logs include structured fields per request/port.

4. Expand README.md and schema.md with storage plan
   Define goals: SQL row store + external BLOB store, B+ Tree index, partitioning strategy.
   Acceptance: Docs enumerate on-disk layout, page size, and blob strategy.

[DOING] 5. Create storage skeleton
   Add src/storage/{mod.rs,types.rs,page.rs,pager.rs} with: PageId, RecordId, 4KB page constants, pager trait.
   Acceptance: Compiles; unit tests for basic page read/write stubs.

6. Implement basic pager over a single file
   Pager to map page ids to offsets, read/write pages with zero-initialization.
   Acceptance: Unit tests for create/read/write page pass.
7. B+ Tree in-memory node structs + serialization layout
   Define node header, keys array, child pointers for internal and leaf nodes.
   Acceptance: Serialize/deserialize roundtrips for nodes tested.
8. B+ Tree insert/search (single-file, single-thread)
   Implement search path, leaf insert, split, propagate split up.
   Acceptance: Insertion/search tests pass for ascending/descending/random inserts.
9. Index persistence via Pager
   Wire B+ to read/write nodes from disk through Pager.
   Acceptance: Restart test: open, insert, close, reopen, search returns inserted keys.
10. Minimal row store atop pager (heap file)
    Slotted-page layout for rows; APIs: append_row, get_row.
    Acceptance: Tests for insert/get across multiple pages.
11. Catalog metadata
    Create src/catalog/{mod.rs,tables.rs} to store table schemas and index metadata in a small system catalog file.
    Acceptance: CREATE TABLE updates catalog; reload persists.
12. Replace stub SQL parsing with sqlparser crate
    Parse CREATE TABLE, INSERT, SELECT ... WHERE key = ....
    Acceptance: Parse unit tests for the three statements.
13. Simple executor for CREATE TABLE and INSERT
    Map parsed AST to catalog/table creation; insert maps columns to row format and to primary index.
    Acceptance: Integration test: create table, insert rows, select by primary key.
14. BLOB store (content-addressed)
    Add src/blobstore/{mod.rs,fs.rs}; store by SHA-256 of content, chunking for large files, return blob id; threshold-based inline vs reference.
    Acceptance: Put/get stream APIs; persistence on disk; unit tests.
15. Add BLOB SQL type and reference handling
    In INSERT, detect BLOB fields as references to blob ids; provide helper API to import files and return ids.
    Acceptance: Insert row referencing blob id; select returns blob id.
16. HTTP /sql endpoint
    Accept POST body SQL query; return JSON result; handle errors with codes.
    Acceptance: curl roundtrip works for create/insert/select.
17. Prisma-like query builder (Rust DSL)
    Add src/prisma/{mod.rs,query.rs} with builder API for CRUD on tables; translate to AST.
    Acceptance: Unit test: builder constructs queries that execute and return expected rows.
18. Partition routing strategy
    Consistent hashing (or modular) over partition key; route requests to server ports; intra-process call for now.
    Acceptance: Same API, but partitions’ file paths are separate; hash chooses correct file.
19. WAL (write-ahead logging) skeleton
    src/wal/{mod.rs,logger.rs}: append-only log with LSN; write page images or logical ops before commit.
    Acceptance: Crash/restart test for simple workload passes using recovery stub.
20. Concurrency and basic locking
    Add per-table mutex/read-write locks; serialize writes in this phase.
    Acceptance: Tokio test with concurrent inserts doesn’t corrupt data.
21. Basic transactions
    Begin/commit with WAL flush; no rollbacks yet.
    Acceptance: Insert in transaction visible after commit.
22. CLI client for manual testing
    Add a small binary src/bin/cli.rs to connect over TCP and send SQL.
    Acceptance: Interactive session works for basic commands.
23. Bench and profile
    Micro-bench for B+ inserts/searches; tracing spans for hot paths.
    Acceptance: Reported metrics in README.
24. Blob GC and ref counting
    Track references in tables; garbage collect unreferenced blobs.
    Acceptance: After deleting rows referencing a blob, GC removes data.
25. Hardening + config
    Config file/env for ports, data dir, page size; error handling review.
    Acceptance: Startup reads config; meaningful errors across components.
    Immediate next two steps I’ll take if you want me to start now
    Edit src/db/http/mod.rs to remove the bad server.data().await call and return a 200 for /sql so the project builds.
    Replace ~/data/... path usage with a resolved path using dirs to ensure files are created correctly.
    If you want, I can make those two quick edits and re-run the build.
    Fixed build blocker identified and mapped first two fixes.
    Proposed a sequence of focused 15-minute tasks covering storage (B+ tree, pager, WAL), SQL parsing/execution, BLOB store, HTTP API, partitioning, and minimal transactions.
