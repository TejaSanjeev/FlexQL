#ifndef FLEXQL_B_PLUS_TREE_H
#define FLEXQL_B_PLUS_TREE_H

#include "storage/buffer_pool.h"
#include "storage/index/b_plus_tree_internal_page.h"
#include "storage/index/b_plus_tree_leaf_page.h"

namespace flexql {
namespace storage {

class BPlusTree {
public:
    // Takes the Buffer Pool to manage disk pages, and the root page ID if reloading from disk
    explicit BPlusTree(BufferPoolManager* bpm, int root_page_id = -1);

    // Core API
    bool search(int key, RecordID& result);
    bool insert(int key, const RecordID& rid);
    
    int get_root_page_id() const { return root_page_id_; }

private:
    BufferPoolManager* bpm_;
    int root_page_id_;

    // Tree Navigation
    Page* find_leaf_page(int key);

    // Insertion Logic
    void start_new_tree(int key, const RecordID& rid);
    bool insert_into_leaf(int key, const RecordID& rid);
    
    // The complex mathematical splitters
    BPlusTreeLeafPage* split_leaf(BPlusTreeLeafPage* leaf_page);
    void insert_into_parent(BPlusTreePage* old_node, int key, BPlusTreePage* new_node);
    
    // THE FIX: Recursive internal splitting for scaling to 10+ Million rows
    void split_internal(BPlusTreeInternalPage* old_internal, int new_key, int right_page_id);
};

} 
} 

#endif