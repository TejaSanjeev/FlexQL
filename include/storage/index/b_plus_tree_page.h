#ifndef FLEXQL_B_PLUS_TREE_PAGE_H
#define FLEXQL_B_PLUS_TREE_PAGE_H

#include <cstdint>

namespace flexql {
namespace storage {

enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

// This header takes up exactly 24 bytes at the start of the 4096-byte page.
class BPlusTreePage {
public:
    bool is_leaf_page() const { return page_type_ == IndexPageType::LEAF_PAGE; }
    bool is_root_page() const { return parent_page_id_ == -1; }
    
    void set_page_type(IndexPageType page_type) { page_type_ = page_type; }
    
    int get_size() const { return size_; }
    void set_size(int size) { size_ = size; }
    void increase_size(int amount) { size_ += amount; }
    
    int get_max_size() const { return max_size_; }
    void set_max_size(int max_size) { max_size_ = max_size; }
    
    int get_parent_page_id() const { return parent_page_id_; }
    void set_parent_page_id(int parent_page_id) { parent_page_id_ = parent_page_id; }
    
    int get_page_id() const { return page_id_; }
    void set_page_id(int page_id) { page_id_ = page_id; }

private:
    IndexPageType page_type_; // 4 bytes
    int size_;                // 4 bytes (Current number of keys)
    int max_size_;            // 4 bytes (Max keys before we must SPLIT)
    int parent_page_id_;      // 4 bytes
    int page_id_;             // 4 bytes
    int padding_;             // 4 bytes (To keep it aligned to 8-byte boundaries)
};

} 
} 

#endif