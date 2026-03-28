#ifndef FLEXQL_TABLE_H
#define FLEXQL_TABLE_H

#include "parser/ast.h"
#include "storage/buffer_pool.h"
#include "storage/index/b_plus_tree.h"
#include <string>
#include <vector>
#include <shared_mutex>
#include <memory>
#include <functional>
#include <unordered_map>

namespace flexql {
namespace storage {

class Table {
public:
    Table(const std::string& name, const std::vector<parser::ColumnDef>& columns, BufferPoolManager* bpm, int btree_root_id = -1);

    bool insert_row(const std::vector<std::string>& raw_values);
    
    std::string select_all(const std::vector<std::string>& select_cols);
    std::string select_by_key(const std::string& key, const std::vector<std::string>& select_cols);

    // THE FIX: An iterator to scan the table for complex operations like JOINs
    void scan_table(std::function<void(const std::unordered_map<std::string, std::string>&)> callback);

    void add_page_id(int page_id);
    const std::vector<int>& get_page_ids() const;
    const std::vector<parser::ColumnDef>& get_columns() const { return columns_; }
    
    int get_btree_root_id() const;
    bool just_allocated_new_page() const { return newly_allocated_; }

    bool delete_all_data();

private:
    std::string name_;
    std::vector<parser::ColumnDef> columns_;

    BufferPoolManager* bpm_;
    std::vector<int> page_ids_;
    mutable std::shared_mutex table_latch_;

    std::unique_ptr<BPlusTree> primary_index_;
    bool newly_allocated_ = false;

    std::vector<char> serialize_row(const std::vector<std::string>& values, long long expiration);
    std::string deserialize_tuple(const Tuple& t, const std::vector<std::string>& select_cols);
    bool is_expired(const Tuple& t);
};

} 
} 

#endif