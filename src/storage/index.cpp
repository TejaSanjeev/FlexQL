#include "storage/index.h"

namespace flexql {
namespace storage {

void Index::insert(const std::string& key, RecordID rid) {
    tree_[key] = rid;
}

bool Index::lookup(const std::string& key, RecordID& out_rid) {
    auto it = tree_.find(key);
    if (it != tree_.end()) {
        out_rid = it->second;
        return true;
    }
    return false;
}

std::vector<RecordID> Index::range_scan(const std::string& min_key, const std::string& max_key) {
    std::vector<RecordID> results;

    auto it_start = tree_.lower_bound(min_key);
    auto it_end = tree_.upper_bound(max_key);

    for (auto it = it_start; it != it_end; ++it) {
        results.push_back(it->second);
    }

    return results;
}

void Index::clear() {
    tree_.clear();
}

std::map<std::string, RecordID> Index::get_all_entries() const {
    return tree_;
}

}
}