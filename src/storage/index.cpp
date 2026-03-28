#include "storage/index.h"
#include <mutex> // Added this to fix the error
#include <shared_mutex>

namespace flexql {
namespace storage {

void Index::insert(const std::string& key, RecordID rid) {
    std::unique_lock<std::shared_mutex> lock(index_latch_);
    tree_[key] = rid; 
}

bool Index::lookup(const std::string& key, RecordID& out_rid) {
    std::shared_lock<std::shared_mutex> lock(index_latch_);
    auto it = tree_.find(key);
    if (it != tree_.end()) {
        out_rid = it->second;
        return true;
    }
    return false;
}

std::vector<RecordID> Index::range_scan(const std::string& min_key, const std::string& max_key) {
    std::shared_lock<std::shared_mutex> lock(index_latch_);
    std::vector<RecordID> results;
    
    auto it_start = tree_.lower_bound(min_key);
    auto it_end = tree_.upper_bound(max_key);

    for (auto it = it_start; it != it_end; ++it) {
        results.push_back(it->second);
    }
    
    return results;
}

} 
}