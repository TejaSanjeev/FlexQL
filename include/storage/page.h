#ifndef FLEXQL_PAGE_H
#define FLEXQL_PAGE_H

#include <cstdint>
#include <cstring>

namespace flexql {
namespace storage {

#define PAGE_SIZE 4096

struct Tuple {
    uint16_t size;
    char* data;
};

class Page {
public:
    Page();
    void init(); // Initializes a fresh page with headers
    
    char* get_raw_data() { return data_; }

    bool has_space(uint16_t tuple_size);
    uint16_t insert_tuple(const char* tuple_data, uint16_t tuple_size);
    Tuple get_tuple(uint16_t slot_id);

    // FIX: Moved to public so Table::select_all can read how many tuples exist
    uint16_t get_tuple_count() const;

private:
    char data_[PAGE_SIZE]; // Everything MUST live in here

    // Helper methods to read/write the header DIRECTLY into the byte array
    void set_tuple_count(uint16_t count);
    
    uint16_t get_free_space_pointer() const;
    void set_free_space_pointer(uint16_t ptr);
};

} 
} 
#endif