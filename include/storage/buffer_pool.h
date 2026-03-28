#ifndef FLEXQL_BUFFER_POOL_H
#define FLEXQL_BUFFER_POOL_H

#include "storage/page.h"
#include "storage/disk_manager.h"
#include <unordered_map>
#include <list>
#include <mutex>
#include <vector>

namespace flexql {
namespace storage {

class BufferPoolManager {
public:
    BufferPoolManager(size_t pool_size, DiskManager* disk_manager);
    ~BufferPoolManager();

    // Fetch a page from RAM or Disk
    Page* fetch_page(int page_id);
    
    // Allocate a brand new page on Disk and bring it to RAM
    Page* new_page(int* out_page_id);
    
    // Tell the pool we are done using the page
    bool unpin_page(int page_id, bool is_dirty);
    
    // Force all dirty pages to disk (useful for safe shutdowns)
    void flush_all_pages();

private:
    size_t pool_size_;
    DiskManager* disk_manager_;
    std::vector<Page*> pages_; 
    
    struct FrameData {
        int page_id = -1;
        int pin_count = 0;
        bool is_dirty = false;
    };
    std::vector<FrameData> frames_;

    std::unordered_map<int, size_t> page_table_; // Maps page_id to frame index
    std::list<size_t> lru_list_; // Tracks least recently used frames

    std::mutex latch_;

    bool find_victim_frame(size_t* out_frame_id);
    void update_lru(size_t frame_id);
};

} 
} 

#endif