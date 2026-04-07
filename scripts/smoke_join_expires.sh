#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

pkill -f flexql-server || true
rm -f /tmp/flexql_join_s.log /tmp/flexql_join_c.log /tmp/flexql_join_input.sql /tmp/flexql_probe.log
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

./bin/flexql-server > /tmp/flexql_join_s.log 2>&1 &
PID=$!
wait_for_server

cat > /tmp/flexql_join_input.sql <<'EOF'
CREATE TABLE L (ID INT, V VARCHAR(10));
CREATE TABLE R (ID INT, EXPIRES_AT DATETIME, NOTE VARCHAR(10));
INSERT INTO L VALUES (1, 'LV');
INSERT INTO R VALUES (1, 9999999999, 'RV');
SELECT * FROM L INNER JOIN R ON L.ID = R.ID;
.exit
EOF

./bin/flexql-client < /tmp/flexql_join_input.sql > /tmp/flexql_join_c.log 2>&1
kill -15 "$PID" || true
sleep 1

echo "---JOIN CLIENT---"
cat /tmp/flexql_join_c.log
