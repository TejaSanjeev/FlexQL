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
    uint16_t slot;
};

class Index {
public:
    void insert(const std::string& key, RecordID rid);
    bool lookup(const std::string& key, RecordID& out_rid);
    std::vector<RecordID> range_scan(const std::string& min_key, const std::string& max_key);

private:
    std::map<std::string, RecordID> tree_;
    mutable std::shared_mutex index_latch_;
};

} 
} 

#endif