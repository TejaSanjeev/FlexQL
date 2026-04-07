#ifndef FLEXQL_AST_H
#define FLEXQL_AST_H

#include <string>
#include <vector>

namespace flexql {
namespace parser {

enum class StmtType {
    CREATE,
    INSERT,
    SELECT,
    DELETE,
    UNKNOWN
};

enum class DataType {
    INT,
    DECIMAL,
    VARCHAR,
    DATETIME
};

struct ColumnDef {
    std::string name;
    std::string type;
    bool is_primary_key = false;
    bool is_auto_injected = false;
};

struct SQLStatement {
    StmtType type = StmtType::UNKNOWN;
    std::string table_name;
    bool create_if_not_exists = false;
    std::vector<ColumnDef> columns;
    std::vector<std::string> insert_values;
    std::vector<std::vector<std::string>> insert_values_list; // Multi-row
    std::vector<std::string> select_columns;

    bool has_where = false;
    std::string where_column;
    std::string where_operator = "=";
    std::string where_value;

    std::string order_by_col = "";
    bool order_desc = false;

    // --- NEW JOIN FIELDS ---
    bool has_join = false;
    std::string join_table;
    std::string join_left_col;
    std::string join_operator = "=";
    std::string join_right_col;
};

} 
} 

#endif