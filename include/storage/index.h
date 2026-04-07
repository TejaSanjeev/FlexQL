#ifndef FLEXQL_INDEX_H
#define FLEXQL_INDEX_H

#include <map>
#include <string>
#include <shared_mutex>
#include <vector>
#include <cstdint>

namespace flexql {
namespace storage {

// CHANGED: Now uses the Disk page_id instead of a volatile RAM Page* pointer
struct RecordID {
    int page_id;
    uint32_t slot; // Changed to uint32_t to support up to 4B row indexes
};

class Index {
public:
    void insert(const std::string& key, RecordID rid);
    bool lookup(const std::string& key, RecordID& out_rid);
    std::vector<RecordID> range_scan(const std::string& min_key, const std::string& max_key);
    void clear();
    std::map<std::string, RecordID> get_all_entries() const;

private:
    std::map<std::string, RecordID> tree_;
};

} 
} 

#endif