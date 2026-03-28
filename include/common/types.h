#ifndef FLEXQL_TYPES_H
#define FLEXQL_TYPES_H

#include <cstdint>

namespace flexql {

// Supported data types for our columns
enum class DataType : uint8_t {
    INT,
    DECIMAL,
    VARCHAR,
    DATETIME
};

// System-wide constants for performance tuning
constexpr size_t PAGE_SIZE = 4096;        // 4KB pages to align with OS
constexpr size_t MAX_COLUMNS = 32;        // Prevent infinite schema allocations
constexpr size_t MAX_STRING_LEN = 255;    // Fixed limits aid memory pooling

} // namespace flexql

#endif // FLEXQL_TYPES_H