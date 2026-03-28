#include "storage/page.h"

namespace flexql {
namespace storage {

Page::Page() { 
    init(); 
}

void Page::init() {
    memset(data_, 0, PAGE_SIZE);
    set_tuple_count(0);
    set_free_space_pointer(PAGE_SIZE); // Data grows backwards from 4096
}

uint16_t Page::get_tuple_count() const {
    uint16_t count;
    memcpy(&count, data_, sizeof(uint16_t)); // Read bytes 0-1
    return count;
}

void Page::set_tuple_count(uint16_t count) {
    memcpy(data_, &count, sizeof(uint16_t)); // Write bytes 0-1
}

uint16_t Page::get_free_space_pointer() const {
    uint16_t ptr;
    memcpy(&ptr, data_ + sizeof(uint16_t), sizeof(uint16_t)); // Read bytes 2-3
    return ptr;
}

void Page::set_free_space_pointer(uint16_t ptr) {
    memcpy(data_ + sizeof(uint16_t), &ptr, sizeof(uint16_t)); // Write bytes 2-3
}

bool Page::has_space(uint16_t tuple_size) {
    uint16_t count = get_tuple_count();
    // 4 bytes for header + (4 bytes per slot map)
    uint16_t free_space = get_free_space_pointer() - (4 + count * 4); 
    return free_space >= (uint16_t)(tuple_size + 4); 
}

uint16_t Page::insert_tuple(const char* tuple_data, uint16_t tuple_size) {
    if (!has_space(tuple_size)) return -1; 

    uint16_t count = get_tuple_count();
    uint16_t ptr = get_free_space_pointer();

    // 1. Move pointer back and write the raw data
    ptr -= tuple_size;
    set_free_space_pointer(ptr);
    memcpy(data_ + ptr, tuple_data, tuple_size);

    // 2. Write the slot mapping (Offset and Length)
    uint16_t slot_offset_pos = 4 + (count * 4);
    memcpy(data_ + slot_offset_pos, &ptr, sizeof(uint16_t));
    memcpy(data_ + slot_offset_pos + sizeof(uint16_t), &tuple_size, sizeof(uint16_t));

    // 3. Update header count
    set_tuple_count(count + 1);
    
    return count; // Returns the slot_id
}

Tuple Page::get_tuple(uint16_t slot_id) {
    uint16_t count = get_tuple_count();
    if (slot_id >= count) return {0, nullptr}; // Does not exist

    uint16_t slot_offset_pos = 4 + (slot_id * 4);
    uint16_t tuple_offset;
    uint16_t tuple_size;
    
    memcpy(&tuple_offset, data_ + slot_offset_pos, sizeof(uint16_t));
    memcpy(&tuple_size, data_ + slot_offset_pos + sizeof(uint16_t), sizeof(uint16_t));

    // Safety checks against corrupted disk bytes
    if (tuple_size == 0 || tuple_offset >= PAGE_SIZE || tuple_offset + tuple_size > PAGE_SIZE) {
         return {0, nullptr}; 
    }

    return {tuple_size, data_ + tuple_offset};
}

} 
}