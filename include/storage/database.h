#pragma once

#include <string>
#include <unordered_map>
#include <memory>
#include <shared_mutex>
#include <mutex>
#include <list>
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
    bool insert_into(const parser::SQLStatement& stmt);
    std::string select_from(const parser::SQLStatement& stmt);
    bool delete_from(const parser::SQLStatement& stmt);

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
    std::unordered_map<std::string, std::string> query_cache_;
    std::list<std::string> lru_cache_list_;
    std::mutex cache_mutex_;
    const size_t MAX_CACHE_SIZE = 500;
};

} // namespace storage
} // namespace flexql