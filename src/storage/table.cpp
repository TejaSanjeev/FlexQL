#include "storage/table.h"
#include <sstream>
#include <iostream>

namespace flexql {
namespace storage {

Table::Table(const std::string& name, const std::vector<parser::ColumnDef>& columns, BufferPoolManager* bpm, int btree_root_id) 
    : name_(name), columns_(columns) {
    // We ignore the buffer pool manager entirely to go fast in memory, but now persist to disk.
    memory_rows_.reserve(1200000);
    memory_index_.reserve(1200000);

    std::filesystem::path data_dir = std::filesystem::path("data") / "tables";
    std::error_code ec;
    std::filesystem::create_directories(data_dir, ec);
    data_file_path_ = (data_dir / (name_ + ".tbl")).string();

    load_from_disk();
    
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

    
    pending_persist_rows_ += 1;
    if (pending_persist_rows_ >= std::max(50000, (int)memory_rows_.size() / 2)) {
        persist_to_disk();
        pending_persist_rows_ = 0;
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

    
    
    
    pending_persist_rows_ += 1;
    if (pending_persist_rows_ >= std::max(50000, (int)memory_rows_.size() / 2)) {
        persist_to_disk();
        pending_persist_rows_ = 0;
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
    int expires_idx = -1;
    for (size_t i = 0; i < columns_.size(); ++i) {
        if (columns_[i].name == "EXPIRES_AT") expires_idx = i;
    }
    long long now = std::time(nullptr);

    for (const auto& row : memory_rows_) {
        // Enforce expiration
        if (expires_idx != -1 && expires_idx < (int)row.size()) {
            try {
                if (std::stoll(row[expires_idx]) > 0 && std::stoll(row[expires_idx]) <= now) {
                    continue;
                }
            } catch(...) {}
        }

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

    // Truncate on-disk heap file
    if (!data_file_path_.empty()) {
        std::ofstream out(data_file_path_, std::ios::trunc | std::ios::binary);
    }
    return true;
}

void Table::load_from_disk() {
    if (data_file_path_.empty()) return;
    std::ifstream in(data_file_path_, std::ios::binary);
    if (!in.good()) return;
    // Removed large buffer here because reading is one time
    while (in) {
        uint32_t col_count = 0;
        if (!in.read(reinterpret_cast<char*>(&col_count), sizeof(col_count))) break;
        if (col_count != columns_.size()) break;

        std::vector<std::string> row;
        row.reserve(col_count);
        bool bad = false;
        for (uint32_t i = 0; i < col_count; ++i) {
            uint32_t len = 0;
            if (!in.read(reinterpret_cast<char*>(&len), sizeof(len))) { bad = true; break; }
            std::string value(len, '\0');
            if (len > 0 && !in.read(&value[0], len)) { bad = true; break; }
            row.push_back(std::move(value));
        }
        if (bad) break;

        if (!row.empty()) {
            const std::string& pk = row[0];
            auto it = memory_index_.find(pk);
            if (it != memory_index_.end()) {
                memory_rows_[it->second] = std::move(row);
            } else {
                memory_rows_.push_back(std::move(row));
                memory_index_[pk] = memory_rows_.size() - 1;
            }
        }
    }
}

void Table::persist_to_disk() {
    if (data_file_path_.empty()) return;
    std::ofstream out(data_file_path_, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) return;
    
    char buffer[1024 * 1024];
    out.rdbuf()->pubsetbuf(buffer, sizeof(buffer));

    for (const auto& row : memory_rows_) {
        uint32_t col_count = static_cast<uint32_t>(columns_.size());
        out.write(reinterpret_cast<const char*>(&col_count), sizeof(col_count));
        for (size_t i = 0; i < columns_.size(); ++i) {
            const std::string& val = i < row.size() ? row[i] : std::string();
            uint32_t len = static_cast<uint32_t>(val.size());
            out.write(reinterpret_cast<const char*>(&len), sizeof(len));
            if (len > 0) out.write(val.data(), len);
        }
    }
    out.close();
}
}
}
