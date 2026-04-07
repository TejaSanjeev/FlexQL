# FlexQL

FlexQL is a SQL-like client-server database driver written in C++.
It supports a focused relational subset with persistent storage, WAL-based recovery, indexing, caching, and multithreaded request handling.

## Highlights
- C/C++ API compatible client library (`flexql_open`, `flexql_close`, `flexql_exec`, `flexql_free`)
- TCP server with thread-pool based concurrency
- SQL subset: `CREATE TABLE`, `INSERT`, `SELECT`, `DELETE`, `WHERE`, `INNER JOIN`, `ORDER BY`
- Persistent table storage in `data/tables`
- WAL for crash recovery in `data/wal/flexql.wal`
- Primary-key indexing with dense numeric optimization path
- LRU query-result caching and row-offset caching
- Expiration-aware reads using `EXPIRES_AT`

## Repository Layout
- `include/`: public and internal headers
- `src/client/`: CLI client, benchmark client, C API implementation
- `src/server/`: server entrypoint and request handling
- `src/parser/`: SQL parsing and AST building
- `src/storage/`: table storage, WAL, page/buffer/disk managers, index
- `src/network/`: TCP server implementation
- `src/concurrency/`: thread pool
- `scripts/`: benchmark and smoke-test scripts
- `data/`: persisted runtime state (tables, wal)
- `design_document.tex`: design write-up in LaTeX

## Build
From project root:

```bash
make clean
make -j
```

Produced binaries:
- `bin/flexql-server`
- `bin/flexql-client`
- `bin/flexql-benchmark`

## Run
### 1) Start server
```bash
./bin/flexql-server
```

### 2) Start client (new terminal)
```bash
./bin/flexql-client 127.0.0.1 9000
```

### 3) Example session
```sql
CREATE TABLE STUDENT (ID INT, NAME VARCHAR(64));
INSERT INTO STUDENT VALUES (1, 'Alice');
INSERT INTO STUDENT VALUES (2, 'Bob');
SELECT * FROM STUDENT;
```

Exit client with:
```text
.exit
```

## Benchmark
Default benchmark inserts 10,000,000 rows (`DEFAULT_INSERT_ROWS` in `src/client/benchmark_flexql.cpp`).

Run benchmark directly:
```bash
./bin/flexql-benchmark
```

Run with explicit row count:
```bash
./bin/flexql-benchmark 10000000
```

## Performance Results (Current Run)

Benchmark configuration:
- Target insert rows: 10,000,000
- Insert batch size in benchmark: 5,000

Run context:
- Connected to FlexQL
- Running SQL subset checks plus insertion benchmark

Detailed insert benchmark values:
- CREATE TABLE BIG_USERS: 18 ms
- Rows inserted: 10,000,000
- Elapsed time: 8,190 ms
- Throughput: 1,221,001 rows/sec

Detailed unit test and validation values:
- CREATE TABLE TEST_USERS: 178 ms
- RESET TEST_USERS: 5 ms
- INSERT TEST_USERS: 0 ms
- Single-row value validation: PASS
- Filtered rows validation: PASS
- Empty result-set validation: PASS
- CREATE TABLE TEST_ORDERS: 15 ms
- RESET TEST_ORDERS: 5 ms
- INSERT TEST_ORDERS: 0 ms
- Join with no matches validation: PASS
- Invalid SQL should fail: PASS
- Missing table should fail: PASS
- Unit Test Summary: 16/16 passed, 0 failed

Note: performance numbers can vary across runs depending on CPU scheduling, background workload, I/O state, and cache warmness.


## Data and Cleanup
`make clean` removes build artifacts and runtime state:
- `bin/*`
- `flexql.db`
- `data/tables/*`
- `data/indexes/*`
- `data/wal/*`


