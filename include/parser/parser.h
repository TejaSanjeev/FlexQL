#pragma once
#include <string>
#include <vector>
#include "parser/ast.h"

namespace flexql {
namespace parser {
    class Parser {
    public:
        static SQLStatement parse(const std::string& query);
    private:
        static std::string trim(const std::string& str);
    };
}
}