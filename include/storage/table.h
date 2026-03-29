#ifndef FLEXQL_TABLE_H
#define FLEXQL_TABLE_H

#include "parser/ast.h"
#include "storage/buffer_pool.h"
#include <string>
#include <vector>
#include <shared_mutex>
#include <memory>
#include <functional>
#include <unordered_map>
#include <fstream>
#include <filesystem>

namespace flexql {
namespace storage {

class Table {
public:
    Table(const std::string& name, const std::vector<parser::ColumnDef>& columns, BufferPoolManager* bpm, int btree_root_id = -1);

    bool insert_row(const std::vector<std::string>& raw_values);
    bool insert_rows(std::vector<std::vector<std::string>>& rows);
    std::string select_all(const std::vector<std::string>& select_cols);
    std::string select_by_key(const std::string& key, const std::vector<std::string>& select_cols);
    void scan_table(std::function<void(const std::unordered_map<std::string, std::string>&)> callback);

    void add_page_id(int page_id) {}
    const std::vector<int>& get_page_ids() const { static std::vector<int> empty; return empty; }
    const std::vector<parser::ColumnDef>& get_columns() const { return columns_; }
    int get_btree_root_id() const { return -1; }
    bool just_allocated_new_page() const { return false; }
    bool delete_all_data();

private:
    std::string name_;
    std::vector<parser::ColumnDef> columns_;
    mutable std::shared_mutex table_latch_;

    // In-Memory Fast Path
    std::vector<std::vector<std::string>> memory_rows_;
    std::unordered_map<std::string, size_t> memory_index_;

    // Durable storage: snapshot-based persistence
    std::string data_file_path_;
    int pending_persist_rows_ = 0;

    void load_from_disk();
    void persist_to_disk();
};

}
}
#endif