CXX = g++
CXXFLAGS = -std=c++17 -pthread -I./include -O3 -march=native

SERVER_SRC = src/network/tcp_server.cpp \
             src/concurrency/thread_pool.cpp \
             src/parser/parser.cpp \
             src/storage/page.cpp \
             src/storage/index/b_plus_tree.cpp \
             src/storage/index/b_plus_tree_internal_page.cpp \
             src/storage/index/b_plus_tree_leaf_page.cpp \
             src/storage/disk_manager.cpp \
             src/storage/buffer_pool.cpp \
             src/storage/table.cpp \
             src/storage/database.cpp \
             src/server/main.cpp

CLIENT_SRC = src/client/main.cpp \
             src/client/libflexql.cpp

BENCHMARK_SRC = src/client/benchmark_flexql.cpp \
                src/client/libflexql.cpp

all: server client benchmark

server:
	@mkdir -p bin
	$(CXX) $(CXXFLAGS) $(SERVER_SRC) -o bin/flexql-server

client:
	@mkdir -p bin
	$(CXX) $(CXXFLAGS) $(CLIENT_SRC) -o bin/flexql-client

benchmark:
	@mkdir -p bin
	$(CXX) $(CXXFLAGS) $(BENCHMARK_SRC) -o bin/flexql-benchmark

clean:
	rm -rf bin/* flexql.db data/tables/* data/indexes/* data/wal/*

