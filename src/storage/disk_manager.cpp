#include "storage/disk_manager.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cstring>
#include <iostream>

namespace flexql {
namespace storage {

DiskManager::DiskManager(const std::string& db_file) : file_name_(db_file) {
    // Open file. Create if it doesn't exist. Open in read/write mode.
    fd_ = open(db_file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd_ < 0) {
        std::cerr << "DiskManager: Failed to open database file.\n";
        exit(1);
    }

    // Calculate how many 4KB pages currently exist in the file
    struct stat stat_buf;
    fstat(fd_, &stat_buf);
    next_page_id_ = stat_buf.st_size / PAGE_SIZE;
}

DiskManager::~DiskManager() {
    if (fd_ >= 0) {
        close(fd_);
    }
}

int DiskManager::allocate_page() {
    // Atomically increment the page counter (thread-safe, lock-free)
    int new_page_id = next_page_id_.fetch_add(1);
    
    // Write 4KB of zeroes to reserve the space on disk
    char empty_page[PAGE_SIZE];
    memset(empty_page, 0, PAGE_SIZE);
    write_page(new_page_id, empty_page);
    
    return new_page_id;
}

void DiskManager::read_page(int page_id, char* page_data) {
    off_t offset = static_cast<off_t>(page_id) * PAGE_SIZE;
    
    // pread is thread-safe. It reads from an offset without moving a global file pointer.
    ssize_t bytes_read = pread(fd_, page_data, PAGE_SIZE, offset);
    
    if (bytes_read != PAGE_SIZE) {
        // If the file is smaller than expected, just zero out the buffer
        memset(page_data, 0, PAGE_SIZE);
    }
}

void DiskManager::write_page(int page_id, const char* page_data) {
    off_t offset = static_cast<off_t>(page_id) * PAGE_SIZE;
    
    // pwrite is thread-safe for concurrent writes to different pages
    ssize_t bytes_written = pwrite(fd_, page_data, PAGE_SIZE, offset);
    
    if (bytes_written != PAGE_SIZE) {
        std::cerr << "DiskManager: Failed to write entire page to disk.\n";
    }
}

void DiskManager::flush_log() {
    // Forces the OS to flush its page cache down to the physical SSD/HDD
    fsync(fd_);
}

} 
}