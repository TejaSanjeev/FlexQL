#include "storage/database.h"
#include <iostream>
#include <sstream>
#include <cstring>
#include <algorithm>
#include <unordered_set>

namespace flexql {
namespace storage {

Database::Database(const std::string& db_name, int buffer_pool_size) 
    : db_name_(db_name) {
    disk_manager_ = std::make_unique<DiskManager>(db_name_);
    bpm_ = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_manager_.get());
    load_master_page();
}

Database::~Database() {
    save_master_page();
    if (bpm_) {
        bpm_->flush_all_pages();
    }
}

bool Database::delete_from(const parser::SQLStatement& stmt) {
    std::shared_ptr<Table> table;
    {
        std::shared_lock<std::shared_mutex> lock(catalog_latch_);
        if (tables_.find(stmt.table_name) == tables_.end()) return false;
        table = tables_[stmt.table_name];
    }
    
    // Invalidate Cache
    {
        std::lock_guard<std::mutex> cache_lock(cache_mutex_);
        query_cache_.clear(); 
        lru_cache_list_.clear();
    }
    
    if (table->delete_all_data()) {
        save_master_page();
        return true;
    }
    return false;
}

bool Database::create_table(const parser::SQLStatement& stmt, const std::string& raw_query) {
    {
        std::lock_guard<std::mutex> cache_lock(cache_mutex_);
        query_cache_.clear();
        lru_cache_list_.clear();
    }

    std::unique_lock<std::shared_mutex> lock(catalog_latch_);
    if (tables_.find(stmt.table_name) != tables_.end()) {
        return false;
    }

    // Pass -1 so the B+ Tree initializes its own root correctly on the first insert
    tables_[stmt.table_name] = std::make_shared<Table>(stmt.table_name, stmt.columns, bpm_.get(), -1);
    table_schemas_[stmt.table_name] = raw_query;

    lock.unlock(); 
    save_master_page();
    
    return true;
}

bool Database::insert_into(parser::SQLStatement& stmt) {
    {
        std::lock_guard<std::mutex> cache_lock(cache_mutex_);
        query_cache_.clear();
        lru_cache_list_.clear();
    }

    std::shared_ptr<Table> table;
    {
        std::shared_lock<std::shared_mutex> lock(catalog_latch_);
        if (tables_.find(stmt.table_name) == tables_.end()) {
            return false;
        }
        table = tables_[stmt.table_name];
    }

    bool success = true;
    if (!stmt.insert_values_list.empty()) {
        success = table->insert_rows(stmt.insert_values_list);
    }

    // Complete durability: only save master page if we really need to track new pages.
    // The buffer pool will flush pages automatically when full.
    if (table->just_allocated_new_page()) {
        save_master_page();
    }

    return success;
}

// Helper to evaluate Math Operators
bool evaluate_where(const std::string& op, const std::string& row_val, const std::string& target_val) {
    if (op == "=") return row_val == target_val;
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
    std::ostringstream key_stream;
    key_stream << stmt.table_name << "|" << stmt.has_join << "|" << stmt.join_table << "|" 
               << stmt.join_operator << "|"
               << stmt.has_where << "|" << stmt.where_column << "|" << stmt.where_operator << "|" << stmt.where_value << "|" << stmt.order_by_col << "|" << stmt.order_desc << "|";
    for (const auto& col : stmt.select_columns) key_stream << col << ",";
    std::string cache_key = key_stream.str();

    {
        std::lock_guard<std::mutex> cache_lock(cache_mutex_);
        if (query_cache_.find(cache_key) != query_cache_.end()) {
            lru_cache_list_.remove(cache_key);
            lru_cache_list_.push_front(cache_key);
            return query_cache_[cache_key]; 
        }
    }

    std::shared_ptr<Table> table;
    std::shared_ptr<Table> right_table;
    
    // Consolidate catalog locks
    {
        std::shared_lock<std::shared_mutex> lock(catalog_latch_);
        if (tables_.find(stmt.table_name) == tables_.end()) return "Error: Primary table not found.\n";
        table = tables_[stmt.table_name];

        if (stmt.has_join) {
            if (tables_.find(stmt.join_table) == tables_.end()) return "Error: Join table not found.\n";
            right_table = tables_[stmt.join_table];
        }
    }

    std::vector<std::unordered_map<std::string, std::string>> results;

    if (stmt.has_join) {
        table->scan_table([&](const std::unordered_map<std::string, std::string>& left_row) {
            // Support Table Prefixes: Add 'TABLE.COL' format to the map
            std::unordered_map<std::string, std::string> full_left = left_row;
            for(auto& pair : left_row) full_left[stmt.table_name + "." + pair.first] = pair.second;

            auto l_join_it = full_left.find(stmt.join_left_col);
            if (l_join_it == full_left.end()) return; 

            right_table->scan_table([&](const std::unordered_map<std::string, std::string>& right_row) {
                std::unordered_map<std::string, std::string> full_right = right_row;
                for(auto& pair : right_row) full_right[stmt.join_table + "." + pair.first] = pair.second;

                auto r_join_it = full_right.find(stmt.join_right_col);
                if (r_join_it == full_right.end()) return;

                if (evaluate_where(stmt.join_operator, l_join_it->second, r_join_it->second)) {
                    std::unordered_map<std::string, std::string> merged = full_left;
                    merged.insert(full_right.begin(), full_right.end());

                    if (stmt.has_where) {
                        if (merged.find(stmt.where_column) == merged.end()) return;
                        if (!evaluate_where(stmt.where_operator, merged[stmt.where_column], stmt.where_value)) return;
                    }
                    results.push_back(merged);
                }
            });
        });
    } else {
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
        for(const auto& col : table->get_columns()) display_cols.push_back(col.name);
        if (stmt.has_join) {
            for(const auto& col : right_table->get_columns()) {
                if(stmt.join_right_col.find(col.name) == std::string::npos) display_cols.push_back(col.name);
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

    int row_idx = 0;
    for (const auto& row : results) {
        oss << "Row " << row_idx++ << ": ";
        bool first = true;
        for (const auto& col : display_cols) {
            if (!first) oss << "\t"; 
            oss << col << "=" << (row.count(col) ? row.at(col) : "NULL");
            first = false;
        }
        oss << "\n";
    }

    std::string result_string = oss.str();
    if (result_string.find("Error") == std::string::npos) {
        std::lock_guard<std::mutex> cache_lock(cache_mutex_);
        if (query_cache_.size() >= MAX_CACHE_SIZE) {
            std::string oldest = lru_cache_list_.back();
            lru_cache_list_.pop_back();
            query_cache_.erase(oldest);
        }
        query_cache_[cache_key] = result_string;
        lru_cache_list_.push_front(cache_key);
    }

    return result_string;
}

void Database::save_master_page() {
    std::shared_lock<std::shared_mutex> lock(catalog_latch_);
    int current_page_id = 0;
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

        int btree_root_id = pair.second->get_btree_root_id();
        write_data(&btree_root_id, sizeof(int));

        const auto& p_ids = pair.second->get_page_ids();
        int total_pages = p_ids.size();
        write_data(&total_pages, sizeof(int));
        
        if (total_pages > 0) {
            write_data(p_ids.data(), total_pages * sizeof(int));
        }
    }

    bpm_->unpin_page(current_page_id, true); 
    if (bpm_) bpm_->flush_all_pages(); 
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

        int btree_root_id;
        if (!read_data(&btree_root_id, sizeof(int))) break;

        int page_count;
        if (!read_data(&page_count, sizeof(int))) break;

        std::vector<int> p_ids(page_count);
        if (page_count > 0) {
            if (!read_data(p_ids.data(), page_count * sizeof(int))) break;
        }

        try {
            auto stmt = parser::Parser::parse(query);
            auto new_table = std::make_shared<Table>(stmt.table_name, stmt.columns, bpm_.get(), btree_root_id);
            for (int pid : p_ids) new_table->add_page_id(pid);
            tables_[stmt.table_name] = new_table;
            table_schemas_[stmt.table_name] = query; 
        } catch (...) { break; }
    }

    bpm_->unpin_page(current_page_id, false);
}

} // namespace storage
} // namespace flexql