#include "storage/table.h"
#include <cstring>
#include <sstream>
#include <mutex>
#include <iostream>
#include <chrono>

namespace flexql {
namespace storage {

Table::Table(const std::string& name, const std::vector<parser::ColumnDef>& columns, BufferPoolManager* bpm, int btree_root_id) 
    : name_(name), columns_(columns), bpm_(bpm) {
    primary_index_ = std::make_unique<BPlusTree>(bpm_, btree_root_id);
}

int Table::get_btree_root_id() const {
    return primary_index_->get_root_page_id();
}

void Table::add_page_id(int page_id) {
    page_ids_.push_back(page_id);
}

const std::vector<int>& Table::get_page_ids() const {
    return page_ids_;
}

// ==========================================================
// STEP 2: EXPIRATION LOGIC
// ==========================================================
bool Table::is_expired(const Tuple& t) {
    if (t.size < sizeof(long long) || t.data == nullptr) return false;
    
    long long expiration;
    memcpy(&expiration, t.data, sizeof(long long));

    if (expiration == 0) return false; // 0 = Never expires

    // Compare with current Unix time
    auto now = std::chrono::system_clock::now().time_since_epoch();
    long long current_sec = std::chrono::duration_cast<std::chrono::seconds>(now).count();

    return current_sec >= expiration;
}

std::vector<char> Table::serialize_row(const std::vector<std::string>& values, long long expiration) {
    std::vector<char> buffer;
    char* exp_ptr = reinterpret_cast<char*>(&expiration);
    buffer.insert(buffer.end(), exp_ptr, exp_ptr + sizeof(long long));

    // Notice we only loop through columns_.size(), ignoring the expiration value if it was appended
    for (size_t i = 0; i < columns_.size(); ++i) {
        uint16_t len = values[i].length();
        char* len_ptr = reinterpret_cast<char*>(&len);
        buffer.insert(buffer.end(), len_ptr, len_ptr + sizeof(uint16_t));
        buffer.insert(buffer.end(), values[i].begin(), values[i].end());
    }
    return buffer;
}

std::string Table::deserialize_tuple(const Tuple& t, const std::vector<std::string>& select_cols) {
    if (t.size == 0 || t.data == nullptr) return "[Empty Tuple]";
    
    std::ostringstream oss;
    size_t offset = sizeof(long long); 
    
    bool select_all = select_cols.empty() || (select_cols.size() == 1 && select_cols[0] == "*");
    
    for (size_t c = 0; c < columns_.size(); ++c) {
        if (offset + sizeof(uint16_t) > t.size) break; 
        
        uint16_t len;
        memcpy(&len, t.data + offset, sizeof(uint16_t));
        offset += sizeof(uint16_t);
        
        if (offset + len > t.size) {
            oss << "[CORRUPTED MEMORY]";
            break; 
        }
        
        std::string val(t.data + offset, len);
        offset += len;
        
        // STEP 1: COLUMN FILTERING LOGIC
        bool include_col = select_all;
        if (!include_col) {
            for (const auto& req_col : select_cols) {
                if (req_col == columns_[c].name) {
                    include_col = true;
                    break;
                }
            }
        }
        
        if (include_col) {
            oss << columns_[c].name << "=" << val << "\t";
        }
    }
    return oss.str();
}

bool Table::insert_row(const std::vector<std::string>& raw_values) {
    long long expiration = 0;
    
    // STEP 2: Check if user provided an extra value for the Expiration Timestamp
    if (raw_values.size() == columns_.size() + 1) {
        try { 
            expiration = std::stoll(raw_values.back()); 
        } catch (...) { 
            return false; 
        }
    } else if (raw_values.size() != columns_.size()) {
        return false; // Wrong number of arguments
    }

    auto buffer = serialize_row(raw_values, expiration);
    newly_allocated_ = false;
    
    std::unique_lock<std::shared_mutex> lock(table_latch_);

    if (page_ids_.empty()) {
        int new_page_id;
        bpm_->new_page(&new_page_id);
        page_ids_.push_back(new_page_id);
        bpm_->unpin_page(new_page_id, true);
        newly_allocated_ = true;
    }

    int current_page_id = page_ids_.back();
    Page* current_page = bpm_->fetch_page(current_page_id);

    if (!current_page->has_space(buffer.size())) {
        bpm_->unpin_page(current_page_id, false);
        current_page = bpm_->new_page(&current_page_id);
        page_ids_.push_back(current_page_id);
        newly_allocated_ = true;
    }

    uint16_t slot_id = current_page->insert_tuple(buffer.data(), buffer.size());
    bpm_->unpin_page(current_page_id, true);

    RecordID rid = { current_page_id, slot_id };
    int key;
    try { 
        key = std::stoi(raw_values[0]); 
    } catch (...) { 
        return false; 
    }
    
    primary_index_->insert(key, rid); 

    return true;
}

std::string Table::select_by_key(const std::string& key, const std::vector<std::string>& select_cols) {
    std::shared_lock<std::shared_mutex> lock(table_latch_);
    RecordID rid;
    int int_key;
    try { int_key = std::stoi(key); } 
    catch (...) { return "Error: Primary key must be an integer.\n"; }

    if (!primary_index_->search(int_key, rid)) return "Row not found.\n";

    
    Page* p = bpm_->fetch_page(rid.page_id);
    if (!p) return "Error: Disk read failed.\n";

    Tuple t = p->get_tuple(rid.slot);
    if (t.size == 0 || t.size > 4096 || t.data == nullptr) {
        bpm_->unpin_page(rid.page_id, false);
        return "Error: Corrupted data at Page " + std::to_string(rid.page_id) + " Slot " + std::to_string(rid.slot) + "\n";
    }

    // STEP 2: Mask expired rows!
    if (is_expired(t)) {
        bpm_->unpin_page(rid.page_id, false);
        return "Row not found.\n"; 
    }

    std::string result = "Found: " + deserialize_tuple(t, select_cols) + "\n";
    bpm_->unpin_page(rid.page_id, false);
    
    return result;
}

std::string Table::select_all(const std::vector<std::string>& select_cols) {
    std::shared_lock<std::shared_mutex> lock(table_latch_);
    std::ostringstream oss;
    int row_count = 0;

    for (int page_id : page_ids_) {
        Page* page = bpm_->fetch_page(page_id);
        for (uint16_t i = 0; i < page->get_tuple_count(); ++i) { 
            Tuple t = page->get_tuple(i);
            if (t.size == 0 || t.size > 4096) continue;
            
            // STEP 2: Mask expired rows!
            if (is_expired(t)) continue; 

            oss << "Row " << ++row_count << ": " << deserialize_tuple(t, select_cols) << "\n";
        }
        bpm_->unpin_page(page_id, false);
    }
    if (row_count == 0) return "No rows found.\n";
    return oss.str();
}
void Table::scan_table(std::function<void(const std::unordered_map<std::string, std::string>&)> callback) {
    std::shared_lock<std::shared_mutex> lock(table_latch_);
    for (int page_id : page_ids_) {
        Page* page = bpm_->fetch_page(page_id);
        
        for (uint16_t i = 0; i < page->get_tuple_count(); ++i) { 
            Tuple t = page->get_tuple(i);
            if (t.size == 0 || t.size > 4096) continue;
            if (is_expired(t)) continue; 

            std::unordered_map<std::string, std::string> row_map;
            size_t offset = sizeof(long long); 
            bool corrupted = false;

            for (size_t c = 0; c < columns_.size(); ++c) {
                if (offset + sizeof(uint16_t) > t.size) { corrupted = true; break; }
                uint16_t len;
                memcpy(&len, t.data + offset, sizeof(uint16_t));
                offset += sizeof(uint16_t);
                
                if (offset + len > t.size) { corrupted = true; break; }
                std::string val(t.data + offset, len);
                offset += len;
                
                row_map[columns_[c].name] = val;
            }
            
            if (!corrupted) {
                callback(row_map); // Send the parsed row to the execution engine
            }
        }
        bpm_->unpin_page(page_id, false);
    }
}

bool Table::delete_all_data() {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    page_ids_.clear();
    // Reset the B+ Tree
    primary_index_ = std::make_unique<BPlusTree>(bpm_, -1);
    newly_allocated_ = true;
    return true;
}
} 
}