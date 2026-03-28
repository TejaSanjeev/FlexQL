#include "parser/parser.h"
#include <sstream>
#include <algorithm>
#include <cctype>

namespace flexql {
namespace parser {

std::string Parser::trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\r\n'\"");
    if (std::string::npos == first) return "";
    size_t last = str.find_last_not_of(" \t\r\n'\"\";");
    return str.substr(first, (last - first + 1));
}

SQLStatement Parser::parse(const std::string& query) {
    SQLStatement stmt;
    std::string q = trim(query);
    
    // Check type prefix
    std::string upper_q = q.substr(0, std::min<size_t>(100, q.length()));
    std::transform(upper_q.begin(), upper_q.end(), upper_q.begin(), ::toupper); 

    // --- DELETE ---
    if (upper_q.find("DELETE FROM ") == 0) {
        stmt.type = StmtType::DELETE;
        size_t end_pos = q.find(';', 12);
        stmt.table_name = trim(q.substr(12, end_pos - 12));
        return stmt;
    }

    // --- CREATE TABLE ---
    if (upper_q.find("CREATE TABLE ") == 0) {
        stmt.type = StmtType::CREATE;
        size_t name_start = 13;
        if (upper_q.find("CREATE TABLE IF NOT EXISTS ") == 0) name_start = 27;  

        size_t paren_start = q.find('(', name_start);
        stmt.table_name = trim(q.substr(name_start, paren_start - name_start)); 

        std::string cols_str = q.substr(paren_start + 1, q.find_last_of(')') - paren_start - 1);
        std::stringstream ss(cols_str);
        std::string col_def;
        while (std::getline(ss, col_def, ',')) {
            std::stringstream css(trim(col_def));
            ColumnDef def;
            css >> def.name >> def.type;
            stmt.columns.push_back(def);
        }
        return stmt;
    }

    // --- INSERT INTO ---
    if (upper_q.find("INSERT INTO ") == 0 || upper_q.find("INSERT ") == 0) {
        stmt.type = StmtType::INSERT;
        size_t table_start = upper_q.find("INTO ") != std::string::npos ? upper_q.find("INTO ") + 5 : 7;
        
        size_t values_pos = -1;
        for (size_t i = table_start; i < q.length() - 6; ++i) {
            if (toupper(q[i]) == 'V' && toupper(q[i+1]) == 'A' && toupper(q[i+2]) == 'L' && 
                toupper(q[i+3]) == 'U' && toupper(q[i+4]) == 'E' && toupper(q[i+5]) == 'S') {
                values_pos = i;
                break;
            }
        }
        
        stmt.table_name = trim(q.substr(table_start, values_pos - table_start));

        const char* p = q.data() + values_pos + 6;
        const char* end = q.data() + q.size();
        
        stmt.insert_values_list.reserve(6000); // Prevent reallocation for massive batches

        while (p < end) {
            while (p < end && *p != '(') p++;
            if (p >= end) break;
            p++; // skip '('

            std::vector<std::string> row_vals;
            row_vals.reserve(10); // reserve a little to prevent reallocation

            while (p < end && *p != ')') {
                while (p < end && std::isspace((unsigned char)*p)) p++;
                if (p >= end || *p == ')') break;

                bool in_quotes = (*p == '\'');
                const char* val_start = p;
                if (in_quotes) {
                    val_start++;
                    p++;
                    while (p < end && *p != '\'') p++;
                } else {
                    while (p < end && *p != ',' && *p != ')' && !std::isspace((unsigned char)*p)) p++;
                }

                row_vals.emplace_back(val_start, p - val_start);

                if (in_quotes && p < end && *p == '\'') p++;
                while (p < end && std::isspace((unsigned char)*p)) p++;
                if (p < end && *p == ',') p++;
            }
            if (!row_vals.empty()) {
                stmt.insert_values_list.push_back(std::move(row_vals));
            }
            if (p < end && *p == ')') p++;
        }

        if (!stmt.insert_values_list.empty()) {
            stmt.insert_values = stmt.insert_values_list[0];
        }
        return stmt;
    }

    // --- SELECT ---
    if (upper_q.find("SELECT ") == 0) {
        stmt.type = StmtType::SELECT;
        
        upper_q = q;
        std::transform(upper_q.begin(), upper_q.end(), upper_q.begin(), ::toupper);

        // 1. Extract ORDER BY
        size_t order_idx = upper_q.find(" ORDER BY ");
        if (order_idx != std::string::npos) {
            std::string order_str = trim(q.substr(order_idx + 10));
            if (order_str.length() >= 4 && order_str.substr(order_str.length() - 4) == "DESC") {
                stmt.order_desc = true;
                stmt.order_by_col = trim(order_str.substr(0, order_str.length() - 4));
            } else {
                if (order_str.length() >= 3 && order_str.substr(order_str.length() - 3) == "ASC") {
                    stmt.order_by_col = trim(order_str.substr(0, order_str.length() - 3));
                } else {
                    stmt.order_by_col = order_str;
                }
            }
            q = q.substr(0, order_idx);
            upper_q = upper_q.substr(0, order_idx);
        }

        // 2. Extract WHERE and Operators
        size_t where_idx = upper_q.find(" WHERE ");
        if (where_idx != std::string::npos) {
            stmt.has_where = true;
            std::string where_str = trim(q.substr(where_idx + 7));
            q = q.substr(0, where_idx);

            std::vector<std::string> ops = {">=", "<=", "!=", "=", ">", "<"};
            for (const auto& op : ops) {
                size_t op_pos = where_str.find(op);
                if (op_pos != std::string::npos) {
                    stmt.where_operator = op;
                    stmt.where_column = trim(where_str.substr(0, op_pos));      
                    stmt.where_value = trim(where_str.substr(op_pos + op.length()));
                    break;
                }
            }
        }

        // 3. Extract FROM and JOIN
        size_t from_idx = upper_q.find(" FROM ");
        std::string cols_str = q.substr(7, from_idx - 7);
        std::stringstream ss(cols_str);
        std::string col;
        while (std::getline(ss, col, ',')) stmt.select_columns.push_back(trim(col));

        std::string from_str = trim(q.substr(from_idx + 6));
        size_t join_idx = upper_q.find(" INNER JOIN ", from_idx);
        if (join_idx != std::string::npos) {
            stmt.has_join = true;
            stmt.table_name = trim(q.substr(from_idx + 6, join_idx - (from_idx + 6)));
            size_t on_idx = upper_q.find(" ON ", join_idx);
            stmt.join_table = trim(q.substr(join_idx + 12, on_idx - (join_idx + 12)));
            std::string on_cond = trim(q.substr(on_idx + 4));
            std::vector<std::string> ops = {">=", "<=", "!=", "=", ">", "<"};   
            for (const auto& op : ops) {
                size_t op_pos = on_cond.find(op);
                if (op_pos != std::string::npos) {
                    stmt.join_operator = op;
                    stmt.join_left_col = trim(on_cond.substr(0, op_pos));       
                    stmt.join_right_col = trim(on_cond.substr(op_pos + op.length()));
                    break;
                }
            }
        } else {
            stmt.table_name = from_str;
        }
        return stmt;
    }

    throw std::runtime_error("Unsupported query type");
}

}
}