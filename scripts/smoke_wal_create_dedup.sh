#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

pkill -f flexql-server || true
rm -f flexql.db data/wal/flexql.wal data/tables/*.tbl /tmp/flexql_wal_s1.log /tmp/flexql_wal_s2.log /tmp/flexql_wal_c1.log /tmp/flexql_wal_c2.log /tmp/flexql_probe.log || true

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

./bin/flexql-server > /tmp/flexql_wal_s1.log 2>&1 &
PID1=$!
wait_for_server
printf "CREATE TABLE WALT (ID INT, V VARCHAR(8));\n.exit\n" | ./bin/flexql-client > /tmp/flexql_wal_c1.log 2>&1
kill -15 "$PID1" || true
sleep 1

./bin/flexql-server > /tmp/flexql_wal_s2.log 2>&1 &
PID2=$!
wait_for_server
printf ".exit\n" | ./bin/flexql-client > /tmp/flexql_wal_c2.log 2>&1
kill -15 "$PID2" || true
sleep 1

COUNT=$(grep -ic 'CREATE TABLE WALT' data/wal/flexql.wal || true)
echo "CREATE_COUNT=$COUNT"
cat data/wal/flexql.wal
