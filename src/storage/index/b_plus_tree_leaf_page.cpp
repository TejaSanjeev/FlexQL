#include "storage/index/b_plus_tree_leaf_page.h"
#include <cstring>

namespace flexql {
namespace storage {

void BPlusTreeLeafPage::init(int page_id, int parent_id, int max_size) {
    set_page_type(IndexPageType::LEAF_PAGE);
    set_page_id(page_id);
    set_parent_page_id(parent_id);
    set_size(0);
    set_max_size(max_size);
    set_next_page_id(-1); // -1 means there is no next page yet
}

int BPlusTreeLeafPage::key_at(int index) const {
    return array_[index].key;
}

RecordID BPlusTreeLeafPage::value_at(int index) const {
    return array_[index].rid;
}

// Inserts a key and keeps the binary array perfectly sorted
int BPlusTreeLeafPage::insert(int key, const RecordID& value) {
    int current_size = get_size();
    
    // 1. Find the exact insertion index using linear search 
    // (Linear is often faster than binary search for small arrays < 400 items due to CPU caching)
    int target_index = 0;
    while (target_index < current_size && array_[target_index].key < key) {
        target_index++;
    }

    // 2. If it's a duplicate primary key, reject it
    if (target_index < current_size && array_[target_index].key == key) {
        return current_size; // Do nothing, return unmodified size
    }

    // 3. Shift all elements right by one to make room
    // memmove is highly optimized at the CPU level for shifting byte blocks safely
    if (target_index < current_size) {
        std::memmove(
            &array_[target_index + 1], 
            &array_[target_index], 
            (current_size - target_index) * sizeof(MappingType)
        );
    }


    // 4. Drop the new data into the slot
    array_[target_index].key = key;
    array_[target_index].rid = value;

    // 5. Update the page header
    increase_size(1);
    
    return get_size();
}

} 
}