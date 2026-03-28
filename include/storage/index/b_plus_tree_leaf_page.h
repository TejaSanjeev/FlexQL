#ifndef FLEXQL_B_PLUS_TREE_LEAF_PAGE_H
#define FLEXQL_B_PLUS_TREE_LEAF_PAGE_H

#include "storage/index/b_plus_tree_page.h"
#include "storage/index.h" 

namespace flexql {
namespace storage {

#define LEAF_PAGE_HEADER_SIZE (sizeof(BPlusTreePage) + sizeof(int))

class BPlusTreeLeafPage : public BPlusTreePage {
public:
    // Max size is usually around 338 for a 4KB page storing (int, RecordID) pairs
    void init(int page_id, int parent_id = -1, int max_size = 338);

    int get_next_page_id() const { return next_page_id_; }
    void set_next_page_id(int next_page_id) { next_page_id_ = next_page_id; }

    int key_at(int index) const;
    RecordID value_at(int index) const;
    
    // Inserts a new key and RecordID, keeping the array perfectly sorted
    int insert(int key, const RecordID& value);

private:
    int next_page_id_; // 4 bytes (Pointer to the next leaf on disk for Range Scans)
    
    struct MappingType {
        int key;
        RecordID rid;
    };
    MappingType array_[0]; 
};

} 
} 

#endif