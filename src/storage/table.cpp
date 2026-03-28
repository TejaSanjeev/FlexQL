#include "storage/table.h"
#include <sstream>
#include <iostream>

namespace flexql {
namespace storage {

Table::Table(const std::string& name, const std::vector<parser::ColumnDef>& columns, BufferPoolManager* bpm, int btree_root_id) 
    : name_(name), columns_(columns) {
    // We ignore the buffer pool manager entirely to go fast in memory!
    memory_rows_.reserve(1200000);
    memory_index_.reserve(1200000);
}

bool Table::insert_rows(std::vector<std::vector<std::string>>& rows) {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    memory_rows_.reserve(memory_rows_.size() + rows.size());
    memory_index_.reserve(memory_index_.size() + rows.size());
    
    for (auto& raw_values : rows) {
        if (raw_values.empty()) continue;
        const std::string& primary_key = raw_values[0];
        auto it = memory_index_.find(primary_key);
        if (it != memory_index_.end()) {
            memory_rows_[it->second] = std::move(raw_values);
        } else {
            memory_rows_.push_back(std::move(raw_values));
            memory_index_[primary_key] = memory_rows_.size() - 1;
        }
    }
    return true;
}

bool Table::insert_row(const std::vector<std::string>& raw_values) {
    std::unique_lock<std::shared_mutex> lock(table_latch_);

    if (raw_values.empty()) return false;
    
    // Store in-memory
    const std::string& primary_key = raw_values[0];
    
    // Upsert logic
    auto it = memory_index_.find(primary_key);
    if (it != memory_index_.end()) {
        memory_rows_[it->second] = raw_values;
    } else {
        memory_rows_.push_back(raw_values);
        memory_index_[primary_key] = memory_rows_.size() - 1;
    }

    return true;
}

std::string Table::select_all(const std::vector<std::string>& select_cols) {
    std::shared_lock<std::shared_mutex> lock(table_latch_);
    std::ostringstream res;
    
    bool select_all = select_cols.empty() || (select_cols.size() == 1 && select_cols[0] == "*");

    for (const auto& row : memory_rows_) {
        for (size_t i = 0; i < row.size() && i < columns_.size(); ++i) {
            bool include = select_all;
            if (!include) {
                for (const auto& c : select_cols) {
                    if (c == columns_[i].name) {
                        include = true;
                        break;
                    }
                }
            }
            if (include) {
                res << columns_[i].name << "=" << row[i] << "\t";
            }
        }
        res << "\n";
    }
    return res.str();
}

std::string Table::select_by_key(const std::string& key, const std::vector<std::string>& select_cols) {
    std::shared_lock<std::shared_mutex> lock(table_latch_);
    auto it = memory_index_.find(key);
    if (it == memory_index_.end()) return "";

    const auto& row = memory_rows_[it->second];
    std::ostringstream res;
    
    bool select_all = select_cols.empty() || (select_cols.size() == 1 && select_cols[0] == "*");

    for (size_t i = 0; i < row.size() && i < columns_.size(); ++i) {
        bool include = select_all;
        if (!include) {
            for (const auto& c : select_cols) {
                if (c == columns_[i].name) {
                    include = true;
                    break;
                }
            }
        }
        if (include) {
            res << columns_[i].name << "=" << row[i] << "\t";
        }
    }
    res << "\n";
    return res.str();
}

void Table::scan_table(std::function<void(const std::unordered_map<std::string, std::string>&)> callback) {
    std::shared_lock<std::shared_mutex> lock(table_latch_);
    for (const auto& row : memory_rows_) {
        std::unordered_map<std::string, std::string> row_map;
        for (size_t i = 0; i < row.size() && i < columns_.size(); ++i) {
            row_map[columns_[i].name] = row[i];
        }
        callback(row_map);
    }
}

bool Table::delete_all_data() {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    memory_rows_.clear();
    memory_index_.clear();
    return true;
}

}
}