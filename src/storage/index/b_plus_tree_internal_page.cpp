#include "storage/index/b_plus_tree_internal_page.h"
#include <iostream>

namespace flexql {
namespace storage {

void BPlusTreeInternalPage::init(int page_id, int parent_id, int max_size) {
    set_page_type(IndexPageType::INTERNAL_PAGE);
    set_page_id(page_id);
    set_parent_page_id(parent_id);
    set_size(0); // Currently holds 0 keys
    set_max_size(max_size);
}

int BPlusTreeInternalPage::key_at(int index) const {
    return array_[index].key;
}

void BPlusTreeInternalPage::set_key_at(int index, int key) {
    array_[index].key = key;
}

int BPlusTreeInternalPage::value_at(int index) const {
    return array_[index].page_id;
}

void BPlusTreeInternalPage::set_value_at(int index, int value) {
    array_[index].page_id = value;
}

// The core routing engine. O(log N) Binary Search.
int BPlusTreeInternalPage::lookup(int key) const {
    // We do a binary search to find the correct child pointer.
    // The array holds pairs like: [(INVALID, P0), (100, P1), (200, P2)]
    // If we search for 150, we want to return P1.
    
    int left = 1;
    int right = get_size() - 1;
    int target_index = 0; // Default to the leftmost child (P0)

    while (left <= right) {
        int mid = left + (right - left) / 2;
        if (array_[mid].key <= key) {
            target_index = mid; // This key is valid, but keep checking the right side
            left = mid + 1;
        } else {
            right = mid - 1; // Key is too big, check the left side
        }
    }

    return array_[target_index].page_id;
}

} 
}