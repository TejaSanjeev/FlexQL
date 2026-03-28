#ifndef FLEXQL_B_PLUS_TREE_INTERNAL_PAGE_H
#define FLEXQL_B_PLUS_TREE_INTERNAL_PAGE_H

#include "storage/index/b_plus_tree_page.h"

namespace flexql {
namespace storage {

// Macro to make pointer arithmetic cleaner
#define INTERNAL_PAGE_HEADER_SIZE sizeof(BPlusTreePage)
#define INTERNAL_PAGE_PAIR_SIZE (sizeof(int) + sizeof(int)) 

class BPlusTreeInternalPage : public BPlusTreePage {
public:
    // Initializes the page when newly allocated from the Buffer Pool
    void init(int page_id, int parent_id = -1, int max_size = 508);

    // Getters and Setters for the Key-Pointer pairs
    int key_at(int index) const;
    void set_key_at(int index, int key);
    
    int value_at(int index) const; // Returns the Page ID of the child node
    void set_value_at(int index, int value);

    // Binary search to find the correct child page to traverse down to in O(log N)
    int lookup(int key) const;

private:
    // A flexible array mapped directly into the remaining 4072 bytes of the page
    // 508 pairs of (Key, PageID) fits perfectly into the 4KB limit.
    struct MappingType {
        int key;
        int page_id;
    };
    MappingType array_[0]; 
};

} 
} 

#endif