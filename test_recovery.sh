./bin/flexql-server > server_recovery.log 2>&1 &
SERVER_PID=$!
sleep 5
echo "SELECT count(*) FROM BIG_USERS;" | ./bin/flexql-client > result.txt
kill -15 $SERVER_PID
