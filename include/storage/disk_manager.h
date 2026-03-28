#ifndef FLEXQL_DISK_MANAGER_H
#define FLEXQL_DISK_MANAGER_H

#include "storage/page.h"
#include <string>
#include <atomic>
#include <mutex>

namespace flexql {
namespace storage {

class DiskManager {
public:
    explicit DiskManager(const std::string& db_file);
    ~DiskManager();

    // Allocates a new logical page ID on disk
    int allocate_page();

    // Thread-safe raw byte reads/writes using pread/pwrite
    void read_page(int page_id, char* page_data);
    void write_page(int page_id, const char* page_data);

    // Forces OS to flush the file cache to physical hardware
    void flush_log();

private:
    int fd_; // File descriptor for the database file
    std::string file_name_;
    std::atomic<int> next_page_id_; // Lock-free atomic counter for new pages
};

} 
} 

#endif