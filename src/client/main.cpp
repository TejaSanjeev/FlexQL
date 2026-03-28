#include <iostream>
#include <string>
#include "flexql.h"

// The required callback function that prints the data dynamically
int print_row(void *data, int columnCount, char **values, char **columnNames) {
    for (int i = 0; i < columnCount; i++) {
        std::cout << columnNames[i] << " = " << (values[i] ? values[i] : "NULL") << "\n";
    }
    std::cout << "\n";
    return 0;
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    int port = 9000;

    if (argc == 3) {
        host = argv[1];
        port = std::stoi(argv[2]);
    }

    FlexQL *db;
    int rc = flexql_open(host.c_str(), port, &db);
    if (rc != FLEXQL_OK) {
        std::cout << "Cannot connect to FlexQL server\n";
        return 1;
    }
    
    std::cout << "Connected to FlexQL server\n";

    std::string line;
    char* errMsg = nullptr;

    while (true) {
        std::cout << "flexql> ";
        if (!std::getline(std::cin, line)) break;
        
        if (line == ".exit" || line == "quit") {
            break;
        }
        if (line.empty()) continue;

        // Execute using the API and the callback
        rc = flexql_exec(db, line.c_str(), print_row, nullptr, &errMsg);
        
        if (rc != FLEXQL_OK) {
            std::cout << "SQL error: " << (errMsg ? errMsg : "Unknown") << "\n";
            flexql_free(errMsg);
            errMsg = nullptr;
        }
    }

    flexql_close(db);
    std::cout << "Connection closed\n";
    return 0;
}