#include "storage/buffer_pool.h"
#include <iostream>
#include <cstring> // Needed for memset

namespace flexql {
namespace storage {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
    pages_.resize(pool_size_);
    frames_.resize(pool_size_);
    for (size_t i = 0; i < pool_size_; ++i) {
        pages_[i] = new Page();
        lru_list_.push_back(i); 
    }
}

BufferPoolManager::~BufferPoolManager() {
    flush_all_pages();
    for (Page* p : pages_) {
        delete p;
    }
}

void BufferPoolManager::update_lru(size_t frame_id) {
    lru_list_.remove(frame_id);
    lru_list_.push_front(frame_id); 
}

bool BufferPoolManager::find_victim_frame(size_t* out_frame_id) {
    for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
        size_t frame_id = *it;
        if (frames_[frame_id].pin_count == 0) {
            *out_frame_id = frame_id;
            return true;
        }
    }
    return false; 
}

Page* BufferPoolManager::fetch_page(int page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    if (page_table_.find(page_id) != page_table_.end()) {
        size_t frame_id = page_table_[page_id];
        frames_[frame_id].pin_count++;
        update_lru(frame_id);
        return pages_[frame_id];
    }

    size_t victim_frame;
    if (!find_victim_frame(&victim_frame)) return nullptr;

    FrameData& old_frame = frames_[victim_frame];
    if (old_frame.page_id != -1) {
        if (old_frame.is_dirty) {
            // FIX 1: Point to the memory array, not the C++ object pointer!
            disk_manager_->write_page(old_frame.page_id, pages_[victim_frame]->get_raw_data());
        }
        page_table_.erase(old_frame.page_id);
    }

    // FIX 2: Safely read data from the SSD directly into the page's inner array
    disk_manager_->read_page(page_id, pages_[victim_frame]->get_raw_data());
    
    page_table_[page_id] = victim_frame;
    old_frame.page_id = page_id;
    old_frame.pin_count = 1;
    old_frame.is_dirty = false;
    update_lru(victim_frame);

    return pages_[victim_frame];
}

Page* BufferPoolManager::new_page(int* out_page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    size_t victim_frame;
    if (!find_victim_frame(&victim_frame)) return nullptr;

    FrameData& old_frame = frames_[victim_frame];
    if (old_frame.page_id != -1) {
        if (old_frame.is_dirty) {
            // FIX 3: Point to the memory array
            disk_manager_->write_page(old_frame.page_id, pages_[victim_frame]->get_raw_data());
        }
        page_table_.erase(old_frame.page_id);
    }

    int page_id = disk_manager_->allocate_page();
    *out_page_id = page_id;

    // FIX 4: Clear the byte array, do not memset the C++ object!
    memset(pages_[victim_frame]->get_raw_data(), 0, PAGE_SIZE);
    pages_[victim_frame]->init(); // Initialize the page header
    
    page_table_[page_id] = victim_frame;
    old_frame.page_id = page_id;
    old_frame.pin_count = 1;
    old_frame.is_dirty = false;
    update_lru(victim_frame);

    return pages_[victim_frame];
}

bool BufferPoolManager::unpin_page(int page_id, bool is_dirty) {
    std::lock_guard<std::mutex> lock(latch_);
    if (page_table_.find(page_id) == page_table_.end()) return false;

    size_t frame_id = page_table_[page_id];
    if (frames_[frame_id].pin_count <= 0) return false;

    frames_[frame_id].pin_count--;
    if (is_dirty) frames_[frame_id].is_dirty = true;
    
    return true;
}

void BufferPoolManager::flush_all_pages() {
    std::lock_guard<std::mutex> lock(latch_);
    for (size_t i = 0; i < pool_size_; ++i) {
        if (frames_[i].page_id != -1 && frames_[i].is_dirty) {
            // FIX 5: Point to the memory array
            disk_manager_->write_page(frames_[i].page_id, pages_[i]->get_raw_data());
            frames_[i].is_dirty = false;
        }
    }
    disk_manager_->flush_log();
}

} 
}