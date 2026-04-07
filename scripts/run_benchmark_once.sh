#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

pkill -f flexql-server || true
rm -f /tmp/flexql_bench_server.log /tmp/flexql_bench_client.log /tmp/flexql_probe.log
rm -f flexql.db data/wal/flexql.wal data/tables/*.tbl || true

wait_for_server() {
    local i=0
    while [ "$i" -lt 300 ]; do
        if printf ".exit\n" | ./bin/flexql-client >/tmp/flexql_probe.log 2>&1; then
            return 0
        fi
        i=$((i + 1))
        sleep 0.2
    done
    return 1
}

./bin/flexql-server > /tmp/flexql_bench_server.log 2>&1 &
SERVER_PID=$!
wait_for_server

/usr/bin/time -f "REAL=%e USER=%U SYS=%S" ./bin/flexql-benchmark 1000000 > /tmp/flexql_bench_client.log 2>&1 || true

kill -15 "$SERVER_PID" || true
sleep 1

echo "=== BENCH CLIENT TAIL ==="
tail -n 80 /tmp/flexql_bench_client.log

echo "=== BENCH SERVER TAIL ==="
tail -n 40 /tmp/flexql_bench_server.log
