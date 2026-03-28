#include "storage/index/b_plus_tree.h"
#include <iostream>

namespace flexql {
namespace storage {

BPlusTree::BPlusTree(BufferPoolManager* bpm, int root_page_id)
    : bpm_(bpm), root_page_id_(root_page_id) {}

// ==========================================================
// SEARCH LOGIC: O(log N) Disk Traversal
// ==========================================================
Page* BPlusTree::find_leaf_page(int key) {
    if (root_page_id_ == -1) return nullptr; 

    Page* curr_page = bpm_->fetch_page(root_page_id_);
    auto curr_node = reinterpret_cast<BPlusTreePage*>(curr_page->get_raw_data());

    while (!curr_node->is_leaf_page()) {
        auto internal_node = reinterpret_cast<BPlusTreeInternalPage*>(curr_node);
        int next_page_id = internal_node->lookup(key);
        
        bpm_->unpin_page(curr_node->get_page_id(), false); 
        
        curr_page = bpm_->fetch_page(next_page_id);        
        curr_node = reinterpret_cast<BPlusTreePage*>(curr_page->get_raw_data());
    }

    return curr_page; 
}

bool BPlusTree::search(int key, RecordID& result) {
    Page* leaf_page = find_leaf_page(key);
    if (leaf_page == nullptr) return false;

    auto leaf_node = reinterpret_cast<BPlusTreeLeafPage*>(leaf_page->get_raw_data());
    bool found = false;

    for (int i = 0; i < leaf_node->get_size(); ++i) {
        if (leaf_node->key_at(i) == key) {
            result = leaf_node->value_at(i);
            found = true;
            break;
        }
    }

    bpm_->unpin_page(leaf_node->get_page_id(), false);
    return found;
}

// ==========================================================
// INSERTION LOGIC
// ==========================================================
void BPlusTree::start_new_tree(int key, const RecordID& rid) {
    Page* root_page = bpm_->new_page(&root_page_id_);
    auto root_node = reinterpret_cast<BPlusTreeLeafPage*>(root_page->get_raw_data());
    
    root_node->init(root_page_id_, -1);
    root_node->insert(key, rid);
    
    bpm_->unpin_page(root_page_id_, true); 
}

bool BPlusTree::insert(int key, const RecordID& rid) {
    if (root_page_id_ == -1) {
        start_new_tree(key, rid);
        return true;
    }
    return insert_into_leaf(key, rid);
}

bool BPlusTree::insert_into_leaf(int key, const RecordID& rid) {
    Page* leaf_page = find_leaf_page(key);
    auto leaf_node = reinterpret_cast<BPlusTreeLeafPage*>(leaf_page->get_raw_data());

    if (leaf_node->get_size() < leaf_node->get_max_size()) {
        leaf_node->insert(key, rid);
        bpm_->unpin_page(leaf_node->get_page_id(), true);
        return true;
    }

    BPlusTreeLeafPage* new_leaf_node = split_leaf(leaf_node);
    
    if (key < new_leaf_node->key_at(0)) {
        leaf_node->insert(key, rid);
    } else {
        new_leaf_node->insert(key, rid);
    }

    insert_into_parent(leaf_node, new_leaf_node->key_at(0), new_leaf_node);

    bpm_->unpin_page(leaf_node->get_page_id(), true);
    bpm_->unpin_page(new_leaf_node->get_page_id(), true);
    return true;
}

// ==========================================================
// SPLIT LOGIC
// ==========================================================
BPlusTreeLeafPage* BPlusTree::split_leaf(BPlusTreeLeafPage* old_leaf) {
    int new_page_id;
    Page* new_page = bpm_->new_page(&new_page_id);
    auto new_leaf = reinterpret_cast<BPlusTreeLeafPage*>(new_page->get_raw_data());
    
    new_leaf->init(new_page_id, old_leaf->get_parent_page_id());

    int split_index = old_leaf->get_size() / 2;
    int items_to_move = old_leaf->get_size() - split_index;

    for (int i = 0; i < items_to_move; ++i) {
        new_leaf->insert(old_leaf->key_at(split_index + i), old_leaf->value_at(split_index + i));
    }
    
    old_leaf->set_size(split_index);

    new_leaf->set_next_page_id(old_leaf->get_next_page_id());
    old_leaf->set_next_page_id(new_leaf->get_page_id());

    return new_leaf;
}

void BPlusTree::insert_into_parent(BPlusTreePage *old_node, int key, BPlusTreePage *new_node) {
    // 1. Root Creation Case
    if (old_node->get_parent_page_id() == -1) {
        int new_page_id;
        Page *new_root_page = bpm_->new_page(&new_page_id);
        auto *new_root = reinterpret_cast<BPlusTreeInternalPage *>(new_root_page->get_raw_data());
        
        new_root->init(new_page_id, -1);
        
        new_root->set_value_at(0, old_node->get_page_id());
        new_root->set_key_at(1, key);
        new_root->set_value_at(1, new_node->get_page_id());
        
        // FIX 1: Size must be 2 (1 key + 2 pointers) so lookup() traverses correctly!
        new_root->set_size(2);
        
        old_node->set_parent_page_id(new_page_id);
        new_node->set_parent_page_id(new_page_id);
        root_page_id_ = new_page_id;
        
        bpm_->unpin_page(new_page_id, true);
        return;
    }

    int parent_id = old_node->get_parent_page_id();
    Page *parent_page = bpm_->fetch_page(parent_id);
    auto *parent_node = reinterpret_cast<BPlusTreeInternalPage *>(parent_page->get_raw_data());

    // 2. Normal Case: Parent has space
    if (parent_node->get_size() < parent_node->get_max_size()) {
        int size = parent_node->get_size();
        int insert_idx = 1;
        
        // FIX 2: Strict inequality (< size) prevents reading uninitialized memory!
        while (insert_idx < size && parent_node->key_at(insert_idx) < key) {
            insert_idx++;
        }
        
        for (int i = size; i >= insert_idx; --i) {
            parent_node->set_key_at(i + 1, parent_node->key_at(i));
            parent_node->set_value_at(i + 1, parent_node->value_at(i));
        }
        
        parent_node->set_key_at(insert_idx, key);
        parent_node->set_value_at(insert_idx, new_node->get_page_id());
        parent_node->set_size(size + 1);

        bpm_->unpin_page(parent_id, true);
        return;
    }

    // 3. Overflow Case
    split_internal(parent_node, key, new_node->get_page_id());
    bpm_->unpin_page(parent_id, true);
}

void BPlusTree::split_internal(BPlusTreeInternalPage* old_internal, int new_key, int right_page_id) {
    int new_page_id;
    Page* new_page = bpm_->new_page(&new_page_id);
    auto new_node = reinterpret_cast<BPlusTreeInternalPage*>(new_page->get_raw_data());
    new_node->init(new_page_id, old_internal->get_parent_page_id());

    // Step 1: Temporarily overflow the old node by 1 index so we can sort it cleanly
    int size = old_internal->get_size();
    int target_idx = 1;
    while (target_idx < size && old_internal->key_at(target_idx) < new_key) {
        target_idx++;
    }
    for (int i = size; i > target_idx; --i) {
        old_internal->set_key_at(i, old_internal->key_at(i - 1));
        old_internal->set_value_at(i, old_internal->value_at(i - 1));
    }
    old_internal->set_key_at(target_idx, new_key);
    old_internal->set_value_at(target_idx, right_page_id);
    size++; // Size is now max_size + 1

    // Step 2: Slice it perfectly in half
    int split_index = size / 2;
    int mid_key = old_internal->key_at(split_index); // This key gets pushed UP

    // Step 3: Move the top half to the new right-hand node
    int j = 1;
    new_node->set_value_at(0, old_internal->value_at(split_index)); 
    
    // Tell the first moved child that it has a new parent
    int child_id = new_node->value_at(0);
    Page* child_page = bpm_->fetch_page(child_id);
    reinterpret_cast<BPlusTreePage*>(child_page->get_raw_data())->set_parent_page_id(new_page_id);
    bpm_->unpin_page(child_id, true);

    for (int i = split_index + 1; i < size; ++i) {
        new_node->set_key_at(j, old_internal->key_at(i));
        new_node->set_value_at(j, old_internal->value_at(i));
        
        // Tell all remaining moved children they have a new parent
        child_id = old_internal->value_at(i);
        child_page = bpm_->fetch_page(child_id);
        reinterpret_cast<BPlusTreePage*>(child_page->get_raw_data())->set_parent_page_id(new_page_id);
        bpm_->unpin_page(child_id, true);
        
        j++;
    }

    new_node->set_size(j);
    old_internal->set_size(split_index); // The mid_key is removed from this level!

    // Step 4: Recursively push the mid_key UP to the next level
    insert_into_parent(old_internal, mid_key, new_node);
    bpm_->unpin_page(new_page_id, true);
}

} 
}