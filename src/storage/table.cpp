#include "storage/table.h"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <ctime>
#include <mutex>
#include <cstring>
#include <cctype>

namespace flexql {
namespace storage {

namespace {
size_t parse_varchar_max_len(const std::string& upper_type) {
    size_t pos1 = upper_type.find('(');
    size_t pos2 = upper_type.find(')');
    if (pos1 == std::string::npos || pos2 == std::string::npos || pos2 <= pos1 + 1) {
        return 0;
    }
    try {
        return static_cast<size_t>(std::stoul(upper_type.substr(pos1 + 1, pos2 - pos1 - 1)));
    } catch (...) {
        return 0;
    }
}
}

Table::Table(const std::string& name, const std::vector<parser::ColumnDef>& columns)
    : name_(name), columns_(columns) {
    bool found_pk = false;
    column_info_.reserve(columns_.size());
    for (size_t i = 0; i < columns_.size(); ++i) {
        ColumnRuntimeInfo info;
        std::string upper_type = columns_[i].type;
        std::transform(upper_type.begin(), upper_type.end(), upper_type.begin(), ::toupper);

        if (upper_type.find("INT") != std::string::npos) {
            info.kind = ColumnKind::INT;
        } else if (upper_type.find("DECIMAL") != std::string::npos) {
            info.kind = ColumnKind::DECIMAL;
        } else if (upper_type.find("VARCHAR") != std::string::npos) {
            info.kind = ColumnKind::VARCHAR;
            info.varchar_max_len = parse_varchar_max_len(upper_type);
        } else if (upper_type.find("DATETIME") != std::string::npos) {
            info.kind = ColumnKind::DATETIME;
        } else {
            info.kind = ColumnKind::OTHER;
        }
        column_info_.push_back(info);

        if (columns_[i].is_primary_key) {
            primary_key_idx_ = i;
            found_pk = true;
        }
        if (columns_[i].name == "EXPIRES_AT") {
            expires_idx_ = static_cast<int>(i);
        }
    }
    if (!found_pk && !columns_.empty()) {
        primary_key_idx_ = 0;
        columns_[0].is_primary_key = true;
    }

    // Hint for dense index eligibility
    std::string pk_type = columns_[primary_key_idx_].type;
    std::transform(pk_type.begin(), pk_type.end(), pk_type.begin(), ::toupper);
    if (pk_type.find("INT") != std::string::npos || pk_type.find("DECIMAL") != std::string::npos || pk_type.find("DATETIME") != std::string::npos) {
        pk_numeric_ = true;
        dense_index_active_ = true;
    }

    std::filesystem::create_directories(std::filesystem::path("data") / "tables");
    data_file_path_ = (std::filesystem::path("data") / "tables" / (name_ + ".tbl")).string();

    append_stream_.open(data_file_path_, std::ios::binary | std::ios::app);
    // Large stream buffer to reduce syscalls
    stream_buffer_.resize(8 << 20);
    append_stream_.rdbuf()->pubsetbuf(stream_buffer_.data(), static_cast<std::streamsize>(stream_buffer_.size()));
    append_stream_.seekp(0, std::ios::end);
    current_offset_ = static_cast<uint64_t>(append_stream_.tellp());
    write_buffer_.reserve(WRITE_BUFFER_FLUSH_BYTES);
    load_from_disk();
}

bool Table::validate_row_shape(const std::vector<std::string>& raw_values) const {
    if (raw_values.size() != columns_.size()) return false;
    if (expires_idx_ == -1) return false; // every row must have expiry
    for (size_t i = 0; i < raw_values.size(); ++i) {
        if (!validate_value_view(raw_values[i], column_info_[i])) return false;
    }
    return true;
}

bool Table::validate_value(const std::string& value, const ColumnRuntimeInfo& info) const {
    return validate_value_view(value, info);
}

bool Table::validate_value_view(std::string_view value, const ColumnRuntimeInfo& info) const {
    if (value.empty() || value == "NULL") return true;

    switch (info.kind) {
        case ColumnKind::INT: {
            size_t pos = 0;
            if (value[0] == '-') {
                if (value.size() == 1) return false;
                pos = 1;
            }
            for (; pos < value.size(); ++pos) {
                if (value[pos] < '0' || value[pos] > '9') return false;
            }
            return true;
        }
        case ColumnKind::DECIMAL: {
            bool dot_seen = false;
            size_t pos = 0;
            if (value[0] == '-') {
                if (value.size() == 1) return false;
                pos = 1;
            }
            for (; pos < value.size(); ++pos) {
                char c = value[pos];
                if (c == '.') {
                    if (dot_seen) return false;
                    dot_seen = true;
                    continue;
                }
                if (c < '0' || c > '9') return false;
            }
            return true;
        }
        case ColumnKind::VARCHAR:
            return info.varchar_max_len == 0 || value.size() <= info.varchar_max_len;
        case ColumnKind::DATETIME: {
            bool all_digits = std::all_of(value.begin(), value.end(), [](char c) {
                return c >= '0' && c <= '9';
            });
            if (all_digits) return true;

            int hyphens = 0;
            int colons = 0;
            for (char c : value) {
                if (c == '-') hyphens++;
                else if (c == ':') colons++;
                else if ((c < '0' || c > '9') && c != ' ' && c != 'T' && c != 'Z' && c != '.') return false;
            }
            return hyphens >= 2 || colons > 0;
        }
        case ColumnKind::OTHER:
        default:
            return true;
    }
}

bool Table::parse_uint64(const std::string& s, uint64_t& out) {
    return parse_uint64_view(s, out);
}

bool Table::parse_uint64_view(std::string_view s, uint64_t& out) {
    if (s.empty()) return false;
    uint64_t val = 0;
    for (char c : s) {
        if (c < '0' || c > '9') return false;
        uint64_t prev = val;
        val = val * 10 + static_cast<uint64_t>(c - '0');
        if (val < prev) return false; // overflow
    }
    out = val;
    return true;
}

bool Table::append_row_locked(const std::vector<std::string>& raw_values) {
    if (!append_stream_.is_open()) {
        append_stream_.open(data_file_path_, std::ios::binary | std::ios::app);
        if (!append_stream_.is_open()) return false;
    }

    uint64_t offset = current_offset_ + static_cast<uint64_t>(write_buffer_.size());

    uint32_t col_count = static_cast<uint32_t>(columns_.size());

    size_t total_len = sizeof(col_count);
    for (const auto& val : raw_values) {
        total_len += sizeof(uint32_t) + val.size();
    }

    size_t start = write_buffer_.size();
    write_buffer_.resize(start + total_len);
    char* dest = write_buffer_.data() + start;

    memcpy(dest, &col_count, sizeof(col_count));
    dest += sizeof(col_count);
    for (const auto& val : raw_values) {
        uint32_t len = static_cast<uint32_t>(val.size());
        memcpy(dest, &len, sizeof(len));
        dest += sizeof(len);
        if (len > 0) {
            memcpy(dest, val.data(), len);
            dest += len;
        }
    }

    const std::string& pk_val = raw_values[primary_key_idx_];
    if (dense_index_active_ && pk_numeric_) {
        uint64_t key_num = 0;
        bool parsed = parse_uint64(pk_val, key_num);
        if (parsed && dense_offsets_.empty()) {
            dense_next_key_ = key_num;
        }

        if (parsed && key_num == dense_next_key_) {
            dense_offsets_.push_back(offset);
            dense_next_key_++;
        } else {
            ensure_map_from_dense();
            dense_index_active_ = false;
            index_[pk_val] = offset;
        }
    } else {
        index_[pk_val] = offset;
    }

    if (write_buffer_.size() >= WRITE_BUFFER_FLUSH_BYTES) {
        flush_buffer_locked();
    }

    return true;
}

bool Table::append_row_views_locked(const std::vector<std::string_view>& raw_values) {
    return append_row_views_locked(raw_values, false, 0);
}

bool Table::append_row_views_locked(const std::vector<std::string_view>& raw_values, bool has_precomputed_key, uint64_t precomputed_key) {
    if (!append_stream_.is_open()) {
        append_stream_.open(data_file_path_, std::ios::binary | std::ios::app);
        if (!append_stream_.is_open()) return false;
    }

    uint64_t offset = current_offset_ + static_cast<uint64_t>(write_buffer_.size());
    uint32_t col_count = static_cast<uint32_t>(columns_.size());

    size_t total_len = sizeof(col_count);
    for (const auto& val : raw_values) {
        total_len += sizeof(uint32_t) + val.size();
    }

    size_t start = write_buffer_.size();
    write_buffer_.resize(start + total_len);
    char* dest = write_buffer_.data() + start;

    memcpy(dest, &col_count, sizeof(col_count));
    dest += sizeof(col_count);
    for (const auto& val : raw_values) {
        uint32_t len = static_cast<uint32_t>(val.size());
        memcpy(dest, &len, sizeof(len));
        dest += sizeof(len);
        if (len > 0) {
            memcpy(dest, val.data(), len);
            dest += len;
        }
    }

    std::string_view pk_val = raw_values[primary_key_idx_];
    if (dense_index_active_ && pk_numeric_) {
        uint64_t key_num = 0;
        bool parsed = false;
        if (has_precomputed_key) {
            key_num = precomputed_key;
            parsed = true;
        } else {
            parsed = parse_uint64_view(pk_val, key_num);
        }

        if (parsed && dense_offsets_.empty()) {
            dense_next_key_ = key_num;
        }

        if (parsed && key_num == dense_next_key_) {
            dense_offsets_.push_back(offset);
            dense_next_key_++;
        } else {
            ensure_map_from_dense();
            dense_index_active_ = false;
            index_[std::string(pk_val)] = offset;
        }
    } else {
        index_[std::string(pk_val)] = offset;
    }

    if (write_buffer_.size() >= WRITE_BUFFER_FLUSH_BYTES) {
        flush_buffer_locked();
    }

    return true;
}

void Table::flush_buffer_locked() {
    if (write_buffer_.empty()) return;
    append_stream_.write(write_buffer_.data(), static_cast<std::streamsize>(write_buffer_.size()));
    current_offset_ += static_cast<uint64_t>(write_buffer_.size());
    write_buffer_.clear();
}

void Table::ensure_map_from_dense() {
    if (!dense_index_active_) return;
    const uint64_t dense_base = dense_next_key_ - static_cast<uint64_t>(dense_offsets_.size());
    for (uint64_t i = 0; i < dense_offsets_.size(); ++i) {
        index_[std::to_string(dense_base + i)] = dense_offsets_[i];
    }
}

bool Table::insert_validated_row_locked(const std::vector<std::string>& raw_values_in) {
    std::vector<std::string> local_vals;
    const std::vector<std::string>* vals_ptr = &raw_values_in;

    if (raw_values_in.size() + 1 == columns_.size() && expires_idx_ == static_cast<int>(columns_.size() - 1)) {
        local_vals = raw_values_in;
        local_vals.push_back("0");
        vals_ptr = &local_vals;
    }

    const std::vector<std::string>& raw_values = *vals_ptr;

    if (!validate_row_shape(raw_values)) return false;

    const std::string& pk = raw_values[primary_key_idx_];
    bool duplicate = false;
    if (dense_index_active_ && pk_numeric_) {
        uint64_t key_num = 0;
        if (parse_uint64(pk, key_num) && !dense_offsets_.empty()) {
            const uint64_t dense_base = dense_next_key_ - static_cast<uint64_t>(dense_offsets_.size());
            if (key_num >= dense_base && key_num < dense_next_key_) {
                duplicate = true;
            }
        }
    } else {
        auto it = index_.find(pk);
        if (it != index_.end() && columns_[primary_key_idx_].is_primary_key) duplicate = true;
    }
    if (duplicate) {
        if (inserts_to_skip_ > 0) {
            inserts_to_skip_--;
            return true;
        }
        return false;
    }

    return append_row_locked(raw_values);
}

bool Table::insert_validated_row_views_locked(const std::vector<std::string_view>& raw_values_in) {
    const std::vector<std::string_view>& raw_values = raw_values_in;

    if (raw_values.size() != columns_.size()) return false;
    if (expires_idx_ == -1) return false;

    for (size_t i = 0; i < raw_values.size(); ++i) {
        if (!validate_value_view(raw_values[i], column_info_[i])) return false;
    }

    std::string_view pk = raw_values[primary_key_idx_];
    bool duplicate = false;
    uint64_t key_num = 0;
    bool has_numeric_key = false;
    if (dense_index_active_ && pk_numeric_) {
        has_numeric_key = parse_uint64_view(pk, key_num);
        if (has_numeric_key && !dense_offsets_.empty()) {
            const uint64_t dense_base = dense_next_key_ - static_cast<uint64_t>(dense_offsets_.size());
            if (key_num >= dense_base && key_num < dense_next_key_) {
                duplicate = true;
            }
        }
    } else {
        auto it = index_.find(std::string(pk));
        if (it != index_.end() && columns_[primary_key_idx_].is_primary_key) duplicate = true;
    }
    if (duplicate) {
        if (inserts_to_skip_ > 0) {
            inserts_to_skip_--;
            return true;
        }
        return false;
    }

    return append_row_views_locked(raw_values, has_numeric_key, key_num);
}

bool Table::insert_rows(std::vector<std::vector<std::string>>& rows) {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    for (auto& row : rows) {
        if (!insert_validated_row_locked(row)) return false;
    }
    return true;
}

bool Table::insert_row(const std::vector<std::string>& raw_values) {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    return insert_validated_row_locked(raw_values);
}

bool Table::insert_values_sql(std::string_view values_sql) {
    std::unique_lock<std::shared_mutex> lock(table_latch_);

    std::vector<std::string_view> row_vals;
    row_vals.reserve(columns_.size());
    auto is_sql_space = [](char ch) {
        return ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t';
    };

    const char* p = values_sql.data();
    const char* end = values_sql.data() + values_sql.size();

    while (p < end) {
        while (p < end && *p != '(') {
            if (*p == ';') return true;
            p++;
        }
        if (p >= end) break;
        p++; // Skip '('

        row_vals.clear();
        while (p < end && *p != ')') {
            while (p < end && is_sql_space(*p)) p++;
            if (p >= end || *p == ')') break;

            char quote = 0;
            if (*p == '\'' || *p == '"') {
                quote = *p;
                p++;
            }

            const char* val_start = p;
            if (quote != 0) {
                while (p < end && *p != quote) p++;
            } else {
                while (p < end && *p != ',' && *p != ')' && !is_sql_space(*p)) p++;
            }

            row_vals.emplace_back(val_start, static_cast<size_t>(p - val_start));

            if (quote != 0 && p < end && *p == quote) p++;
            while (p < end && is_sql_space(*p)) p++;
            if (p < end && *p == ',') p++;
        }

        if (p < end && *p == ')') p++;

        if (row_vals.size() + 1 == columns_.size() && expires_idx_ == static_cast<int>(columns_.size() - 1)) {
            row_vals.push_back("0");
        }
        if (!row_vals.empty() && !insert_validated_row_views_locked(row_vals)) {
            return false;
        }
    }

    return true;
}

bool Table::read_row_at_offset(uint64_t offset, std::vector<std::string>& out_row) const {
    {
        std::lock_guard<std::mutex> cache_lock(row_cache_mutex_);
        auto it = row_cache_.find(offset);
        if (it != row_cache_.end()) {
            row_cache_lru_.erase(it->second.second);
            row_cache_lru_.push_front(offset);
            it->second.second = row_cache_lru_.begin();
            out_row = it->second.first;
            return true;
        }
    }

    std::lock_guard<std::mutex> stream_lock(read_stream_mutex_);
    if (!read_stream_.is_open()) {
        read_stream_.open(data_file_path_, std::ios::binary);
    }
    if (!read_stream_.is_open()) return false;

    read_stream_.clear();
    read_stream_.seekg(static_cast<std::streamoff>(offset));
    uint32_t col_count = 0;
    if (!read_stream_.read(reinterpret_cast<char*>(&col_count), sizeof(col_count))) return false;
    if (col_count != columns_.size()) return false;

    out_row.clear();
    out_row.reserve(col_count);
    for (uint32_t i = 0; i < col_count; ++i) {
        uint32_t len = 0;
        if (!read_stream_.read(reinterpret_cast<char*>(&len), sizeof(len))) return false;
        std::string val(len, '\0');
        if (len > 0 && !read_stream_.read(&val[0], len)) return false;
        out_row.push_back(std::move(val));
    }

    {
        std::lock_guard<std::mutex> cache_lock(row_cache_mutex_);
        auto it = row_cache_.find(offset);
        if (it != row_cache_.end()) {
            row_cache_lru_.erase(it->second.second);
        }
        row_cache_lru_.push_front(offset);
        row_cache_[offset] = {out_row, row_cache_lru_.begin()};

        while (row_cache_.size() > ROW_CACHE_CAPACITY) {
            uint64_t old = row_cache_lru_.back();
            row_cache_lru_.pop_back();
            row_cache_.erase(old);
        }
    }

    return true;
}

void Table::scan_table(std::function<void(const std::unordered_map<std::string, std::string>&)> callback) {
    {
        std::unique_lock<std::shared_mutex> lock(table_latch_);
        flush_buffer_locked();
        append_stream_.flush();
    }

    std::shared_lock<std::shared_mutex> lock(table_latch_);

    std::lock_guard<std::mutex> stream_lock(scan_stream_mutex_);
    if (!scan_stream_.is_open()) {
        scan_stream_.open(data_file_path_, std::ios::binary);
    }
    if (!scan_stream_.is_open()) return;

    scan_stream_.clear();
    scan_stream_.seekg(0, std::ios::beg);

    long long now = std::chrono::seconds(std::time(nullptr)).count();

    while (true) {
        uint32_t col_count = 0;
        if (!scan_stream_.read(reinterpret_cast<char*>(&col_count), sizeof(col_count))) break;
        if (col_count != columns_.size()) break;

        std::vector<std::string> row;
        row.reserve(col_count);
        bool bad = false;
        for (uint32_t i = 0; i < col_count; ++i) {
            uint32_t len = 0;
            if (!scan_stream_.read(reinterpret_cast<char*>(&len), sizeof(len))) { bad = true; break; }
            std::string val(len, '\0');
            if (len > 0 && !scan_stream_.read(&val[0], len)) { bad = true; break; }
            row.push_back(std::move(val));
        }
        if (bad) break;

        if (expires_idx_ >= 0 && expires_idx_ < static_cast<int>(row.size())) {
            try {
                long long exp_ts = std::stoll(row[expires_idx_]);
                if (exp_ts > 0 && exp_ts <= now) continue;
            } catch (...) {}
        }

        std::unordered_map<std::string, std::string> row_map;
        for (size_t i = 0; i < row.size(); ++i) {
            row_map[columns_[i].name] = row[i];
        }
        callback(row_map);
    }
}

void Table::scan_table_by_key(const std::string& key, std::function<void(const std::unordered_map<std::string, std::string>&)> callback) {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    flush_buffer_locked();
    append_stream_.flush();
    uint64_t offset = UINT64_MAX;
    if (dense_index_active_ && pk_numeric_) {
        uint64_t key_num = 0;
        if (parse_uint64(key, key_num) && !dense_offsets_.empty()) {
            const uint64_t dense_base = dense_next_key_ - static_cast<uint64_t>(dense_offsets_.size());
            if (key_num >= dense_base && key_num < dense_next_key_) {
                offset = dense_offsets_[static_cast<size_t>(key_num - dense_base)];
            }
        }
    }
    if (offset == UINT64_MAX) {
        auto it = index_.find(key);
        if (it == index_.end()) return;
        offset = it->second;
    }

    std::vector<std::string> row;
    if (!read_row_at_offset(offset, row)) return;

    long long now = std::chrono::seconds(std::time(nullptr)).count();
    if (expires_idx_ >= 0 && expires_idx_ < static_cast<int>(row.size())) {
        try {
            long long exp_ts = std::stoll(row[expires_idx_]);
            if (exp_ts > 0 && exp_ts <= now) return;
        } catch (...) {}
    }

    std::unordered_map<std::string, std::string> row_map;
    for (size_t i = 0; i < row.size(); ++i) {
        row_map[columns_[i].name] = row[i];
    }
    callback(row_map);
}

bool Table::delete_all_data() {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    append_stream_.close();
    if (read_stream_.is_open()) read_stream_.close();
    if (scan_stream_.is_open()) scan_stream_.close();
    {
        std::lock_guard<std::mutex> cache_lock(row_cache_mutex_);
        row_cache_.clear();
        row_cache_lru_.clear();
    }
    std::ofstream out(data_file_path_, std::ios::binary | std::ios::trunc);
    out.close();
    append_stream_.open(data_file_path_, std::ios::binary | std::ios::app);
    index_.clear();
    write_buffer_.clear();
    current_offset_ = 0;
    dense_offsets_.clear();
    dense_next_key_ = 0;
    dense_index_active_ = pk_numeric_;
    return true;
}

void Table::flush() {
    std::unique_lock<std::shared_mutex> lock(table_latch_);
    flush_buffer_locked();
    append_stream_.flush();
}

void Table::load_from_disk() {
    index_.clear();
    dense_offsets_.clear();
    dense_next_key_ = 0;
    dense_index_active_ = pk_numeric_;
    size_t loaded_rows = 0;
    std::ifstream in(data_file_path_, std::ios::binary);
    if (!in.is_open()) return;

    while (true) {
        uint64_t offset = static_cast<uint64_t>(in.tellg());
        uint32_t col_count = 0;
        if (!in.read(reinterpret_cast<char*>(&col_count), sizeof(col_count))) break;
        if (col_count != columns_.size()) break;

        std::vector<std::string> row;
        row.reserve(col_count);
        bool bad = false;
        for (uint32_t i = 0; i < col_count; ++i) {
            uint32_t len = 0;
            if (!in.read(reinterpret_cast<char*>(&len), sizeof(len))) { bad = true; break; }
            std::string val(len, '\0');
            if (len > 0 && !in.read(&val[0], len)) { bad = true; break; }
            row.push_back(std::move(val));
        }
        if (bad || row.size() <= primary_key_idx_) break;
        const std::string& pk = row[primary_key_idx_];
        if (dense_index_active_ && pk_numeric_) {
            uint64_t key_num = 0;
            bool parsed = parse_uint64(pk, key_num);
            if (parsed && dense_offsets_.empty()) {
                dense_next_key_ = key_num;
            }

            if (parsed && key_num == dense_next_key_) {
                dense_offsets_.push_back(offset);
                dense_next_key_++;
            } else {
                ensure_map_from_dense();
                dense_index_active_ = false;
                index_[pk] = offset;
            }
        } else {
            index_[pk] = offset;
        }
        loaded_rows++;
    }
    
    inserts_to_skip_ = loaded_rows;
}

}  // namespace storage
}  // namespace flexql
