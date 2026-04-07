#!/usr/bin/env bash
set -uo pipefail

cd "$(dirname "$0")/.."

pkill -f flexql-server || true
rm -f /tmp/flexql_c1.log /tmp/flexql_c2.log /tmp/flexql_s1.log /tmp/flexql_s2.log /tmp/flexql_probe.log
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

./bin/flexql-server > /tmp/flexql_s1.log 2>&1 &
PID1=$!
wait_for_server

printf "CREATE TABLE T (ID INT, NAME VARCHAR(32));\nINSERT INTO T VALUES (1, 'A'), (2, 'B');\nSELECT * FROM T;\n.exit\n" | ./bin/flexql-client > /tmp/flexql_c1.log 2>&1
kill -9 "$PID1" || true
sleep 1

./bin/flexql-server > /tmp/flexql_s2.log 2>&1 &
PID2=$!
wait_for_server

printf "SELECT * FROM T;\n.exit\n" | ./bin/flexql-client > /tmp/flexql_c2.log 2>&1
kill -15 "$PID2" || true
sleep 1

echo "---CLIENT2---"
cat /tmp/flexql_c2.log

echo "---SERVER2---"
tail -n 20 /tmp/flexql_s2.log
