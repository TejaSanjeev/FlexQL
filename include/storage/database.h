#pragma once

#include <string>
#include <unordered_map>
#include <memory>
#include <shared_mutex>
#include <mutex>
#include <list>
#include <vector>
#include <string_view>
#include <unordered_set>
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
    bool insert_into_raw_sql(const std::string& raw_query);
    std::string select_from(const parser::SQLStatement& stmt);
    bool delete_from(const parser::SQLStatement& stmt, const std::string& raw_query = "");

    // Asynchronous WAL
    void append_to_wal(const std::string& query);
    void flush_wal();
    void recover_from_wal();

private:
    void save_master_page();
    void load_master_page();
    void invalidate_table_cache(const std::string& table_name);
    void evict_cache_key_locked(const std::string& cache_key);
    void register_cache_key_locked(const std::string& cache_key, const std::vector<std::string>& tables);
    bool insert_into_table_values_sql(const std::string& table_name, std::string_view values_sql, const std::string& wal_record);

    std::string db_name_;
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> bpm_;

    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
    std::unordered_map<std::string, std::string> table_schemas_;
    std::shared_mutex catalog_latch_;

    // Query Result Cache (LRU)
    std::unordered_map<std::string, std::pair<std::string, std::list<std::string>::iterator>> query_cache_;
    std::list<std::string> lru_cache_list_;
    std::unordered_map<std::string, std::unordered_set<std::string>> table_to_cache_keys_;
    std::unordered_map<std::string, std::vector<std::string>> cache_key_tables_;
    std::mutex cache_mutex_;
    const size_t MAX_CACHE_SIZE = 500;

    // Async WAL structures
    size_t wal_pending_records_ = 0;
    std::mutex wal_mutex_;
    int wal_fd_ = -1;
    bool is_recovering_ = false;
    const size_t WAL_BATCH_SIZE = 256;
};

} // namespace storage
} // namespace flexql