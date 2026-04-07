#include "storage/database.h"
#include <iostream>
#include <sstream>
#include <cstring>
#include <algorithm>
#include <unordered_set>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <ctime>
#include <cctype>
#ifdef _WIN32
#include <io.h>
#endif

#if !defined(_WIN32)
extern "C" int fsync(int);
#endif

namespace flexql {
namespace storage {

namespace {
bool write_all_fd(int fd, std::string_view data) {
    const char* ptr = data.data();
    size_t left = data.size();
    while (left > 0) {
        ssize_t written = ::write(fd, ptr, left);
        if (written <= 0) return false;
        ptr += written;
        left -= static_cast<size_t>(written);
    }
    return true;
}

bool has_expires_at_column(const std::shared_ptr<Table>& table) {
    if (!table) return false;
    for (const auto& col : table->get_columns()) {
        if (col.name == "EXPIRES_AT") return true;
    }
    return false;
}

std::string trim_copy(const std::string& s) {
    size_t first = s.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return "";
    size_t last = s.find_last_not_of(" \t\r\n;");
    return s.substr(first, last - first + 1);
}

bool starts_with_insert(const std::string& line) {
    size_t pos = line.find_first_not_of(" \t\r\n");
    if (pos == std::string::npos) return false;
    static const char* kInsert = "INSERT";
    for (size_t i = 0; kInsert[i] != '\0'; ++i) {
        if (pos + i >= line.size()) return false;
        if (std::toupper(static_cast<unsigned char>(line[pos + i])) != kInsert[i]) return false;
    }
    return true;
}

void sync_fd(int fd) {
    if (fd < 0) return;
#ifdef _WIN32
    _commit(fd);
#else
    ::fsync(fd);
#endif
}

size_t find_ci(const std::string& haystack, const char* needle, size_t start_pos = 0) {
    const size_t needle_len = std::strlen(needle);
    if (needle_len == 0 || start_pos >= haystack.size()) return std::string::npos;
    if (needle_len > haystack.size()) return std::string::npos;

    for (size_t i = start_pos; i + needle_len <= haystack.size(); ++i) {
        bool matched = true;
        for (size_t j = 0; j < needle_len; ++j) {
            char a = static_cast<char>(std::toupper(static_cast<unsigned char>(haystack[i + j])));
            char b = static_cast<char>(std::toupper(static_cast<unsigned char>(needle[j])));
            if (a != b) {
                matched = false;
                break;
            }
        }
        if (matched) return i;
    }
    return std::string::npos;
}

std::string compact_sql_payload(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    bool in_single = false;
    bool in_double = false;
    for (char c : s) {
        if (c == '\'' && !in_double) {
            in_single = !in_single;
            out.push_back(c);
            continue;
        }
        if (c == '"' && !in_single) {
            in_double = !in_double;
            out.push_back(c);
            continue;
        }
        if (!in_single && !in_double && (c == ' ' || c == '\t' || c == '\n' || c == '\r')) {
            continue;
        }
        out.push_back(c);
    }
    return out;
}

std::string make_insert_wal_record(const std::string& table_name, std::string_view values_sql) {
    if (values_sql.size() > 4096) {
        return "I\t" + table_name + "\t" + std::string(values_sql);
    }
    return "I\t" + table_name + "\t" + compact_sql_payload(values_sql);
}
}

Database::Database(const std::string& db_name, int buffer_pool_size) 
    : db_name_(db_name) {
    disk_manager_ = std::make_unique<DiskManager>(db_name_);
    bpm_ = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_manager_.get());
    load_master_page();

    std::filesystem::create_directories("data/wal");
    wal_fd_ = ::open("data/wal/flexql.wal", O_CREAT | O_APPEND | O_WRONLY, 0644);
    recover_from_wal();
}

Database::~Database() {
    flush_wal();

    {
        std::shared_lock<std::shared_mutex> lock(catalog_latch_);
        for (auto& pair : tables_) {
            pair.second->flush();
        }
    }

    if (wal_fd_ >= 0) ::close(wal_fd_);

    save_master_page();
    if (bpm_) {
        bpm_->flush_all_pages();
    }
}

void Database::evict_cache_key_locked(const std::string& cache_key) {
    auto cache_it = query_cache_.find(cache_key);
    if (cache_it != query_cache_.end()) {
        lru_cache_list_.erase(cache_it->second.second);
        query_cache_.erase(cache_it);
    }

    auto tables_it = cache_key_tables_.find(cache_key);
    if (tables_it != cache_key_tables_.end()) {
        for (const auto& table_name : tables_it->second) {
            auto map_it = table_to_cache_keys_.find(table_name);
            if (map_it != table_to_cache_keys_.end()) {
                map_it->second.erase(cache_key);
                if (map_it->second.empty()) {
                    table_to_cache_keys_.erase(map_it);
                }
            }
        }
        cache_key_tables_.erase(tables_it);
    }
}

void Database::register_cache_key_locked(const std::string& cache_key, const std::vector<std::string>& tables) {
    cache_key_tables_[cache_key] = tables;
    for (const auto& table_name : tables) {
        table_to_cache_keys_[table_name].insert(cache_key);
    }
}

void Database::invalidate_table_cache(const std::string& table_name) {
    std::lock_guard<std::mutex> cache_lock(cache_mutex_);
    auto it = table_to_cache_keys_.find(table_name);
    if (it == table_to_cache_keys_.end()) return;

    std::vector<std::string> keys(it->second.begin(), it->second.end());
    for (const auto& key : keys) {
        evict_cache_key_locked(key);
    }
}

bool Database::insert_into_table_values_sql(const std::string& table_name, std::string_view values_sql, const std::string& wal_record) {
    invalidate_table_cache(table_name);

    std::shared_ptr<Table> table;
    {
        std::shared_lock<std::shared_mutex> lock(catalog_latch_);
        auto it = tables_.find(table_name);
        if (it == tables_.end()) return false;
        table = it->second;
    }

    append_to_wal(wal_record);
    return table->insert_values_sql(values_sql);
}

void Database::append_to_wal(const std::string& query) {
    if (is_recovering_ || query.empty()) return;
    
    std::lock_guard<std::mutex> lock(wal_mutex_);

    if (wal_fd_ < 0) {
        wal_fd_ = ::open("data/wal/flexql.wal", O_CREAT | O_APPEND | O_WRONLY, 0644);
        if (wal_fd_ < 0) return;
    }

    if (!write_all_fd(wal_fd_, query)) return;
    if (!write_all_fd(wal_fd_, "\n")) return;

    wal_pending_records_++;

    if (wal_pending_records_ >= WAL_BATCH_SIZE && wal_fd_ >= 0) {
        sync_fd(wal_fd_);
        wal_pending_records_ = 0;
    }
}

void Database::flush_wal() {
    if (is_recovering_) return;

    std::lock_guard<std::mutex> lock(wal_mutex_);
    if (wal_pending_records_ == 0) return;

    if (wal_fd_ < 0) {
        wal_fd_ = ::open("data/wal/flexql.wal", O_CREAT | O_APPEND | O_WRONLY, 0644);
        if (wal_fd_ < 0) return;
    }

    sync_fd(wal_fd_);
    wal_pending_records_ = 0;
}

void Database::recover_from_wal() {
    is_recovering_ = true;
    std::ifstream in_wal("data/wal/flexql.wal");
    if (in_wal.is_open()) {
        std::string line;
        while (std::getline(in_wal, line)) {
            if (line.empty()) continue;

            if (line.size() > 2 && line[1] == '\t') {
                if (line[0] == 'I') {
                    size_t sep = line.find('\t', 2);
                    if (sep != std::string::npos) {
                        std::string table_name = line.substr(2, sep - 2);
                        std::string_view values_sql(line.data() + sep + 1, line.size() - sep - 1);
                        std::shared_ptr<Table> table;
                        {
                            std::shared_lock<std::shared_mutex> lock(catalog_latch_);
                            auto it = tables_.find(table_name);
                            if (it != tables_.end()) table = it->second;
                        }
                        if (table) {
                            (void)table->insert_values_sql(values_sql);
                        }
                    }
                    continue;
                }
                if (line[0] == 'C') {
                    std::string raw = line.substr(2);
                    try {
                        auto stmt = parser::Parser::parse(raw);
                        if (stmt.type == parser::StmtType::CREATE) create_table(stmt, raw);
                    } catch (...) {
                        // Ignore malformed WAL line.
                    }
                    continue;
                }
                if (line[0] == 'D') {
                    std::string raw = line.substr(2);
                    try {
                        auto stmt = parser::Parser::parse(raw);
                        if (stmt.type == parser::StmtType::DELETE) delete_from(stmt, raw);
                    } catch (...) {
                        // Ignore malformed WAL line.
                    }
                    continue;
                }
            }

            if (starts_with_insert(line)) {
                (void)insert_into_raw_sql(line);
                continue;
            }
            try {
                auto stmt = parser::Parser::parse(line);
                if (stmt.type == parser::StmtType::CREATE) create_table(stmt, line);
                else if (stmt.type == parser::StmtType::DELETE) delete_from(stmt, line);
            } catch (...) {
                // Ignore parse errors from corrupted lines
            }
        }
        in_wal.close();
    }

    // Checkpoint recovering data to disk
    {
        std::shared_lock<std::shared_mutex> lock(catalog_latch_);
        for (auto& pair : tables_) {
            pair.second->flush();
        }
    }
    save_master_page();
    if (bpm_) {
        bpm_->flush_all_pages();
    }

    // Atomically rewrite WAL with current schema to avoid losing WAL on crash during truncation.
    if (wal_fd_ >= 0) {
        ::close(wal_fd_);
        wal_fd_ = -1;
    }

    const std::string wal_path = "data/wal/flexql.wal";
    const std::string wal_tmp_path = wal_path + ".tmp";
    int tmp_fd = ::open(wal_tmp_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (tmp_fd >= 0) {
        bool ok = true;
        {
            std::shared_lock<std::shared_mutex> lock(catalog_latch_);
            for (const auto& pair : table_schemas_) {
                std::string line = "C\t" + pair.second + "\n";
                if (!write_all_fd(tmp_fd, line)) {
                    ok = false;
                    break;
                }
            }
        }
        if (ok) {
            sync_fd(tmp_fd);
        }
        ::close(tmp_fd);

        if (ok) {
            std::error_code ec;
            std::filesystem::rename(wal_tmp_path, wal_path, ec);
            if (ec) {
                std::filesystem::remove(wal_path, ec);
                ec.clear();
                std::filesystem::rename(wal_tmp_path, wal_path, ec);
                if (ec) {
                    std::filesystem::remove(wal_tmp_path, ec);
                }
            }
        } else {
            std::error_code ec;
            std::filesystem::remove(wal_tmp_path, ec);
        }
    }

    wal_fd_ = ::open(wal_path.c_str(), O_CREAT | O_APPEND | O_WRONLY, 0644);
    
    is_recovering_ = false;
}

bool Database::delete_from(const parser::SQLStatement& stmt, const std::string& raw_query) {
    std::shared_ptr<Table> table;
    {
        std::shared_lock<std::shared_mutex> lock(catalog_latch_);
        if (tables_.find(stmt.table_name) == tables_.end()) return false;
        table = tables_[stmt.table_name];
    }
    
    invalidate_table_cache(stmt.table_name);
    
    if (table->delete_all_data()) {
        save_master_page();
        append_to_wal("D\t" + raw_query);
        flush_wal();
        return true;
    }
    return false;
}

bool Database::create_table(const parser::SQLStatement& stmt, const std::string& raw_query) {
    invalidate_table_cache(stmt.table_name);

    std::unique_lock<std::shared_mutex> lock(catalog_latch_);
    if (tables_.find(stmt.table_name) != tables_.end()) {
        return false;
    }

    auto actual_columns = stmt.columns;
    bool has_expires = false;
    for (const auto& col : actual_columns) {
        if (col.name == "EXPIRES_AT") has_expires = true;
    }
    if (!has_expires) {
        parser::ColumnDef expires_col;
        expires_col.name = "EXPIRES_AT";
        expires_col.type = "DATETIME";
        expires_col.is_primary_key = false;
        expires_col.is_auto_injected = true;
        actual_columns.push_back(expires_col);
    }

    tables_[stmt.table_name] = std::make_shared<Table>(stmt.table_name, actual_columns);
    table_schemas_[stmt.table_name] = raw_query;

    lock.unlock();
    save_master_page();
    append_to_wal("C\t" + raw_query);
    flush_wal();

    return true;
}

bool Database::insert_into(parser::SQLStatement& stmt, const std::string& raw_query) {
    if (!raw_query.empty()) {
        size_t values_pos = find_ci(raw_query, "VALUES");
        if (values_pos != std::string::npos) {
            std::string_view values_sql(raw_query.data() + values_pos + 6, raw_query.size() - (values_pos + 6));
            return insert_into_table_values_sql(stmt.table_name, values_sql, make_insert_wal_record(stmt.table_name, values_sql));
        }
    }

    // Fallback path when raw SQL is unavailable: serialize parsed values once and reuse fast path.
    if (stmt.insert_values_list.empty()) return false;

    std::string generated_values;
    generated_values.reserve(stmt.insert_values_list.size() * 32);
    for (size_t r = 0; r < stmt.insert_values_list.size(); ++r) {
        if (r > 0) generated_values.push_back(',');
        generated_values.push_back('(');
        const auto& row = stmt.insert_values_list[r];
        for (size_t c = 0; c < row.size(); ++c) {
            if (c > 0) generated_values.push_back(',');
            generated_values.push_back('\'');
            for (char ch : row[c]) {
                if (ch == '\'') generated_values.push_back('\'');
                generated_values.push_back(ch);
            }
            generated_values.push_back('\'');
        }
        generated_values.push_back(')');
    }
    return insert_into_table_values_sql(stmt.table_name, generated_values, make_insert_wal_record(stmt.table_name, generated_values));
}

bool Database::insert_into_raw_sql(const std::string& raw_query) {
    size_t into_pos = find_ci(raw_query, "INTO ");
    size_t values_pos = find_ci(raw_query, "VALUES", into_pos == std::string::npos ? 0 : into_pos + 5);
    if (into_pos == std::string::npos || values_pos == std::string::npos || values_pos <= into_pos + 5) {
        return false;
    }

    std::string table_name = trim_copy(raw_query.substr(into_pos + 5, values_pos - (into_pos + 5)));
    if (table_name.empty()) return false;

    std::string_view values_sql = std::string_view(raw_query).substr(values_pos + 6);
    return insert_into_table_values_sql(table_name, values_sql, make_insert_wal_record(table_name, values_sql));
}

// Helper to evaluate Math Operators
bool evaluate_where(const std::string& op, const std::string& row_val, const std::string& target_val) {
    if (op == "=") {
        if (row_val == target_val) return true;
        try { return std::stod(row_val) == std::stod(target_val); } catch(...) {}
        return false;
    }
    try {
        double r = std::stod(row_val);
        double t = std::stod(target_val);
        if (op == ">") return r > t;
        if (op == "<") return r < t;
        if (op == ">=") return r >= t;
        if (op == "<=") return r <= t;
        if (op == "!=") return r != t;
    } catch (...) {
        if (op == ">") return row_val > target_val;
        if (op == "<") return row_val < target_val;
        if (op == ">=") return row_val >= target_val;
        if (op == "<=") return row_val <= target_val;
        if (op == "!=") return row_val != target_val;
    }
    return false;
}

std::string Database::select_from(const parser::SQLStatement& stmt) {
    std::shared_ptr<Table> table;
    std::shared_ptr<Table> right_table;

    {
        std::shared_lock<std::shared_mutex> lock(catalog_latch_);
        if (tables_.find(stmt.table_name) == tables_.end()) return "Error: Primary table not found.\n";
        table = tables_[stmt.table_name];

        if (stmt.has_join) {
            if (tables_.find(stmt.join_table) == tables_.end()) return "Error: Join table not found.\n";
            right_table = tables_[stmt.join_table];
        }
    }

    bool time_sensitive = has_expires_at_column(table) || has_expires_at_column(right_table);
    std::vector<std::string> cache_tables{stmt.table_name};
    if (stmt.has_join && stmt.join_table != stmt.table_name) {
        cache_tables.push_back(stmt.join_table);
    }

    std::ostringstream key_stream;
    key_stream << stmt.table_name << "|" << stmt.has_join << "|" << stmt.join_table << "|" 
               << stmt.join_operator << "|"
               << stmt.has_where << "|" << stmt.where_column << "|" << stmt.where_operator << "|" << stmt.where_value << "|" << stmt.order_by_col << "|" << stmt.order_desc << "|";
    for (const auto& col : stmt.select_columns) key_stream << col << ",";
    if (time_sensitive) {
        key_stream << "|TS=" << static_cast<long long>(std::time(nullptr));
    }
    std::string cache_key = key_stream.str();

    {
        std::lock_guard<std::mutex> cache_lock(cache_mutex_);
        auto it = query_cache_.find(cache_key);
        if (it != query_cache_.end()) {
            lru_cache_list_.erase(it->second.second);
            lru_cache_list_.push_front(cache_key);
            it->second.second = lru_cache_list_.begin();
            return it->second.first;
        }
    }

    std::vector<std::unordered_map<std::string, std::string>> results;

    if (stmt.has_join) {
        const bool is_self_join = (table.get() == right_table.get());

        std::string r_raw_col = stmt.join_right_col;
        size_t dot_pos = r_raw_col.find('.');
        if (dot_pos != std::string::npos) r_raw_col = r_raw_col.substr(dot_pos + 1);

        bool use_right_idx = false;
        if (stmt.join_operator == "=" && !right_table->get_columns().empty() && right_table->get_columns()[0].name == r_raw_col) {
            use_right_idx = true;
        }

        std::vector<std::unordered_map<std::string, std::string>> right_rows_cache;
        if (is_self_join) {
            right_table->scan_table([&](const std::unordered_map<std::string, std::string>& right_row) {
                right_rows_cache.push_back(right_row);
            });
        }

        table->scan_table([&](const std::unordered_map<std::string, std::string>& left_row) {
            // Support Table Prefixes: Add 'TABLE.COL' format to the map
            std::unordered_map<std::string, std::string> full_left = left_row;
            for(auto& pair : left_row) full_left[stmt.table_name + "." + pair.first] = pair.second;

            auto l_join_it = full_left.find(stmt.join_left_col);
            if (l_join_it == full_left.end()) return;

            auto process_right_row = [&](const std::unordered_map<std::string, std::string>& right_row) {
                std::unordered_map<std::string, std::string> full_right = right_row;
                for(auto& pair : right_row) full_right[stmt.join_table + "." + pair.first] = pair.second;

                auto r_join_it = full_right.find(stmt.join_right_col);
                if (r_join_it == full_right.end()) return;

                if (evaluate_where(stmt.join_operator, l_join_it->second, r_join_it->second)) {
                    std::unordered_map<std::string, std::string> merged = full_left;
                    for (const auto& pair : full_right) {
                        merged[pair.first] = pair.second;
                    }

                    if (stmt.has_where) {
                        if (merged.find(stmt.where_column) == merged.end()) return;
                        if (!evaluate_where(stmt.where_operator, merged[stmt.where_column], stmt.where_value)) return;
                    }
                    results.push_back(merged);
                }
            };

            if (is_self_join) {
                for (const auto& cached_right_row : right_rows_cache) {
                    process_right_row(cached_right_row);
                }
            } else if (use_right_idx) {
                right_table->scan_table_by_key(l_join_it->second, process_right_row);
            } else {
                right_table->scan_table(process_right_row);
            }
        });
    } else {
        bool used_index = false;
        if (stmt.has_where && stmt.where_operator == "=") {
            std::string pk_col_name = table->get_columns().empty() ? "" : table->get_columns()[0].name;
            if (stmt.where_column == pk_col_name || stmt.where_column == (stmt.table_name + "." + pk_col_name)) {
                used_index = true;
                table->scan_table_by_key(stmt.where_value, [&](const std::unordered_map<std::string, std::string>& row) {
                    std::unordered_map<std::string, std::string> full_row = row;        
                    for(auto& pair : row) full_row[stmt.table_name + "." + pair.first] = pair.second;
                    results.push_back(full_row);
                });
            }
        }
        
        if (!used_index) {
            table->scan_table([&](const std::unordered_map<std::string, std::string>& row) {
                std::unordered_map<std::string, std::string> full_row = row;        
                for(auto& pair : row) full_row[stmt.table_name + "." + pair.first] = pair.second;

                if (stmt.has_where) {
                    if (full_row.find(stmt.where_column) == full_row.end()) return; 
                    if (!evaluate_where(stmt.where_operator, full_row[stmt.where_column], stmt.where_value)) return;
                }
                results.push_back(full_row);
            });
        }
    }

    // Process ORDER BY
    if (!stmt.order_by_col.empty()) {
        std::sort(results.begin(), results.end(), [&](const auto& a, const auto& b) {
            std::string val_a = a.count(stmt.order_by_col) ? a.at(stmt.order_by_col) : "";
            std::string val_b = b.count(stmt.order_by_col) ? b.at(stmt.order_by_col) : "";
            try {
                double da = std::stod(val_a);
                double db = std::stod(val_b);
                return stmt.order_desc ? (da > db) : (da < db);
            } catch(...) {
                return stmt.order_desc ? (val_a > val_b) : (val_a < val_b);
            }
        });
    }

    // Process Space-Delimited Output Format for the Benchmark
    std::ostringstream oss;
    std::vector<std::string> display_cols;
    if (stmt.select_columns.empty() || (stmt.select_columns.size() == 1 && stmt.select_columns[0] == "*")) {
        for(const auto& col : table->get_columns()) {
            if (!col.is_auto_injected) display_cols.push_back(col.name);
        }
        if (stmt.has_join) {
            std::string r_col = stmt.join_right_col;
            size_t dot_pos = r_col.find('.');
            if (dot_pos != std::string::npos) r_col = r_col.substr(dot_pos + 1);

            for(const auto& col : right_table->get_columns()) {
                if(col.name != r_col && !col.is_auto_injected) display_cols.push_back(col.name);
            }
        }
    } else {
        display_cols = stmt.select_columns;
        // Verify columns exist
        std::unordered_set<std::string> valid_cols;
        for(const auto& col : table->get_columns()) valid_cols.insert(col.name);
        if (stmt.has_join) {
            for(const auto& col : right_table->get_columns()) valid_cols.insert(col.name);
        }
        for (const auto& col : display_cols) {
            // Support TABLE.COL format bypass or basic verify
            if (col.find('.') == std::string::npos && valid_cols.find(col) == valid_cols.end()) {
                return "Error: Unknown column " + col;
            }
        }
    }

    // We no longer prefix with "Row X: " as the API should receive clean data rows
    for (const auto& row : results) {
        bool first = true;
        for (const auto& col : display_cols) {
            if (!first) oss << "\t"; 
            oss << col << "=" << (row.count(col) ? row.at(col) : "NULL");       
            first = false;
        }
        oss << "\n";
    }

    std::string result_string = oss.str();
    // If there were no results, explicitly return "OK\n" so the client knows it succeeded cleanly.
    if (result_string.empty()) return "OK\n";
    if (result_string.find("Error") == std::string::npos) {
        std::lock_guard<std::mutex> cache_lock(cache_mutex_);
        if (query_cache_.size() >= MAX_CACHE_SIZE) {
            std::string oldest = lru_cache_list_.back();
            evict_cache_key_locked(oldest);
        }
        lru_cache_list_.push_front(cache_key);
        query_cache_[cache_key] = {result_string, lru_cache_list_.begin()};
        register_cache_key_locked(cache_key, cache_tables);
    }

    return result_string;
}

void Database::save_master_page() {
    std::shared_lock<std::shared_mutex> lock(catalog_latch_);
    int current_page_id = 0;
    std::vector<int> touched_pages;
    touched_pages.push_back(current_page_id);
    Page* curr_page = bpm_->fetch_page(current_page_id);
    char* ptr = curr_page->get_raw_data();
    
    char* page_end = curr_page->get_raw_data() + PAGE_SIZE - sizeof(int); 
    memset(curr_page->get_raw_data(), 0, PAGE_SIZE); 
    
    int next_page = -1;
    memcpy(curr_page->get_raw_data() + PAGE_SIZE - sizeof(int), &next_page, sizeof(int));

    auto write_data = [&](const void* data, size_t size) {
        const char* data_ptr = static_cast<const char*>(data);
        while (size > 0) {
            size_t space_left = page_end - ptr;
            if (space_left == 0) {
                int new_page_id;
                Page* overflow_page = bpm_->new_page(&new_page_id);
                memset(overflow_page->get_raw_data(), 0, PAGE_SIZE);
                
                memcpy(curr_page->get_raw_data() + PAGE_SIZE - sizeof(int), &new_page_id, sizeof(int));
                
                bpm_->unpin_page(current_page_id, true);
                current_page_id = new_page_id;
                touched_pages.push_back(current_page_id);
                curr_page = overflow_page;
                ptr = curr_page->get_raw_data();
                page_end = curr_page->get_raw_data() + PAGE_SIZE - sizeof(int);
                
                memcpy(curr_page->get_raw_data() + PAGE_SIZE - sizeof(int), &next_page, sizeof(int));
                space_left = page_end - ptr;
            }
            size_t chunk = std::min(size, space_left);
            memcpy(ptr, data_ptr, chunk);
            ptr += chunk;
            data_ptr += chunk;
            size -= chunk;
        }
    };

    int table_count = tables_.size();
    write_data(&table_count, sizeof(int));

    for (const auto& pair : tables_) {
        std::string query = table_schemas_[pair.first];
        int query_len = query.length();

        write_data(&query_len, sizeof(int));
        write_data(query.c_str(), query_len);
    }

    bpm_->unpin_page(current_page_id, true); 
    if (bpm_) {
        for (int page_id : touched_pages) {
            bpm_->flush_page(page_id);
        }
    }
}

void Database::load_master_page() {
    std::unique_lock<std::shared_mutex> lock(catalog_latch_); 
    int current_page_id = 0;
    Page* curr_page = bpm_->fetch_page(current_page_id);
    char* ptr = curr_page->get_raw_data();
    char* page_end = curr_page->get_raw_data() + PAGE_SIZE - sizeof(int);

    auto read_data = [&](void* dest, size_t size) -> bool {
        char* dest_ptr = static_cast<char*>(dest);
        while (size > 0) {
            size_t space_left = page_end - ptr;
            if (space_left == 0) {
                int next_page_id;
                memcpy(&next_page_id, curr_page->get_raw_data() + PAGE_SIZE - sizeof(int), sizeof(int));
                bpm_->unpin_page(current_page_id, false);
                
                if (next_page_id == -1 || next_page_id == 0) return false; 
                
                current_page_id = next_page_id;
                curr_page = bpm_->fetch_page(current_page_id);
                ptr = curr_page->get_raw_data();
                page_end = curr_page->get_raw_data() + PAGE_SIZE - sizeof(int);
                space_left = page_end - ptr;
            }
            size_t chunk = std::min(size, space_left);
            memcpy(dest_ptr, ptr, chunk);
            ptr += chunk;
            dest_ptr += chunk;
            size -= chunk;
        }
        return true;
    };

    int table_count;
    if (!read_data(&table_count, sizeof(int))) { bpm_->unpin_page(current_page_id, false); return; }
    if (table_count < 0 || table_count > 1000) { bpm_->unpin_page(current_page_id, false); return; }

    for (int i = 0; i < table_count; ++i) {
        int query_len;
        if (!read_data(&query_len, sizeof(int))) break;

        std::string query(query_len, '\0');
        if (!read_data(&query[0], query_len)) break;

        try {
            auto stmt = parser::Parser::parse(query);
            auto actual_columns = stmt.columns;
            bool has_expires = false;
            for (const auto& col : actual_columns) {
                if (col.name == "EXPIRES_AT") has_expires = true;
            }
            if (!has_expires) {
                parser::ColumnDef expires_col;
                expires_col.name = "EXPIRES_AT";
                expires_col.type = "DATETIME";
                expires_col.is_primary_key = false;
                expires_col.is_auto_injected = true;
                actual_columns.push_back(expires_col);
            }

            auto new_table = std::make_shared<Table>(stmt.table_name, actual_columns);
            tables_[stmt.table_name] = new_table;
            table_schemas_[stmt.table_name] = query;
        } catch (...) { break; }
    }

    bpm_->unpin_page(current_page_id, false);
}

} // namespace storage
} // namespace flexql