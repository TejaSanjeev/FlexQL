#pragma once

#include <string>
#include <unordered_map>
#include <memory>
#include <shared_mutex>
#include <mutex>
#include <list>
#include <vector>
#include <fstream>
#include "storage/table.h"
#include "storage/buffer_pool.h"
#include "storage/disk_manager.h"
#include "parser/parser.h"

namespace flexql {
namespace storage {

class Database {
public:
    // FIX: Default constructor restored so `Database db;` works in main.cpp
    Database(const std::string& db_name = "flexql.db", int buffer_pool_size = 1024);
    ~Database();

    // FIX: Restored original public APIs and bool return types
    bool create_table(const parser::SQLStatement& stmt, const std::string& raw_query);
    bool insert_into(parser::SQLStatement& stmt, const std::string& raw_query = "");
    std::string select_from(const parser::SQLStatement& stmt);
    bool delete_from(const parser::SQLStatement& stmt, const std::string& raw_query = "");

    // Asynchronous WAL
    void append_to_wal(const std::string& query);
    void flush_wal();
    void recover_from_wal();

private:
    void save_master_page();
    void load_master_page();

    std::string db_name_;
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> bpm_;

    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
    std::unordered_map<std::string, std::string> table_schemas_;
    std::shared_mutex catalog_latch_;

    // Query Result Cache (LRU)
    std::unordered_map<std::string, std::pair<std::string, std::list<std::string>::iterator>> query_cache_;
    std::list<std::string> lru_cache_list_;
    std::mutex cache_mutex_;
    const size_t MAX_CACHE_SIZE = 500;

    // Async WAL structures
    std::vector<std::string> wal_buffer_;
    std::mutex wal_mutex_;
    std::ofstream wal_file_;
    bool is_recovering_ = false;
};

} // namespace storage
} // namespace flexql