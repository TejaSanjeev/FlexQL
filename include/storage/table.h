#ifndef FLEXQL_TABLE_H
#define FLEXQL_TABLE_H

#include "parser/ast.h"
#include <string>
#include <vector>
#include <shared_mutex>
#include <mutex>
#include <memory>
#include <functional>
#include <unordered_map>
#include <list>
#include <fstream>
#include <filesystem>
#include <cstdint>
#include <string_view>

namespace flexql {
namespace storage {

class Table {
public:
    Table(const std::string& name, const std::vector<parser::ColumnDef>& columns);

    bool insert_row(const std::vector<std::string>& raw_values);
    bool insert_rows(std::vector<std::vector<std::string>>& rows);
    bool insert_values_sql(std::string_view values_sql);
    void scan_table(std::function<void(const std::unordered_map<std::string, std::string>&)> callback);
    void scan_table_by_key(const std::string& key, std::function<void(const std::unordered_map<std::string, std::string>&)> callback);
    bool delete_all_data();
    void flush();

    const std::vector<parser::ColumnDef>& get_columns() const { return columns_; }

private:
    enum class ColumnKind {
        INT,
        DECIMAL,
        VARCHAR,
        DATETIME,
        OTHER
    };

    struct ColumnRuntimeInfo {
        ColumnKind kind = ColumnKind::OTHER;
        size_t varchar_max_len = 0;
    };

    std::string name_;
    std::vector<parser::ColumnDef> columns_;
    std::vector<ColumnRuntimeInfo> column_info_;
    mutable std::shared_mutex table_latch_;

    // On-disk heap file and primary-key index (offset-based)
    std::string data_file_path_;
    std::ofstream append_stream_;
    mutable std::ifstream read_stream_;
    mutable std::ifstream scan_stream_;
    mutable std::mutex read_stream_mutex_;
    mutable std::mutex scan_stream_mutex_;
    mutable std::mutex row_cache_mutex_;
    mutable std::list<uint64_t> row_cache_lru_;
    mutable std::unordered_map<uint64_t, std::pair<std::vector<std::string>, std::list<uint64_t>::iterator>> row_cache_;
    std::unordered_map<std::string, uint64_t> index_; // key -> byte offset in heap file
    std::vector<char> write_buffer_;
    std::vector<char> stream_buffer_;
    uint64_t current_offset_ = 0; // file end position excluding buffered bytes
    const size_t WRITE_BUFFER_FLUSH_BYTES = 64 * 1024 * 1024; // 64MB batches further reduce flush overhead on heavy insert workloads
    const size_t ROW_CACHE_CAPACITY = 2048;

    size_t inserts_to_skip_ = 0; // Skip re-inserting during WAL replay

    // Dense primary-key offset index for monotonic numeric keys
    bool pk_numeric_ = false;
    bool dense_index_active_ = false;
    uint64_t dense_next_key_ = 0;
    std::vector<uint64_t> dense_offsets_;

    size_t primary_key_idx_ = 0;
    int expires_idx_ = -1;

    bool append_row_locked(const std::vector<std::string>& raw_values);
    bool append_row_views_locked(const std::vector<std::string_view>& raw_values);
    bool append_row_views_locked(const std::vector<std::string_view>& raw_values, bool has_precomputed_key, uint64_t precomputed_key);
    bool insert_validated_row_locked(const std::vector<std::string>& raw_values);
    bool insert_validated_row_views_locked(const std::vector<std::string_view>& raw_values);
    bool validate_value(const std::string& value, const ColumnRuntimeInfo& info) const;
    bool validate_value_view(std::string_view value, const ColumnRuntimeInfo& info) const;
    void ensure_map_from_dense();
    void flush_buffer_locked();
    bool read_row_at_offset(uint64_t offset, std::vector<std::string>& out_row) const;
    void load_from_disk();
    bool validate_row_shape(const std::vector<std::string>& raw_values) const;
    static bool parse_uint64(const std::string& s, uint64_t& out);
    static bool parse_uint64_view(std::string_view s, uint64_t& out);
};

}
}
#endif