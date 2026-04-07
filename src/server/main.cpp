#include "network/tcp_server.h"
#include "concurrency/thread_pool.h"
#include "parser/parser.h"
#include "storage/database.h"
#include <iostream>
#include <unistd.h>
#include <sys/socket.h>
#include <cstring>
#include <thread>
#include <cctype>
#include <vector>

namespace {
bool starts_with_keyword_ci(const std::string& text, const char* keyword) {
    size_t pos = text.find_first_not_of(" \t\r\n");
    if (pos == std::string::npos) return false;
    for (size_t i = 0; keyword[i] != '\0'; ++i) {
        if (pos + i >= text.size()) return false;
        if (std::toupper(static_cast<unsigned char>(text[pos + i])) != keyword[i]) return false;
    }
    return true;
}
}

void handle_client(int client_fd, flexql::storage::Database& db) {
    std::vector<char> read_buffer(8 * 1024 * 1024);
    char* buffer = read_buffer.data();
    size_t buffer_cap = read_buffer.size();
    
    while (true) {
        std::string query;
        query.reserve(256 * 1024);
        while (true) {
            ssize_t bytes_read = read(client_fd, buffer, buffer_cap - 1);
            if (bytes_read <= 0) break;
            
            buffer[bytes_read] = '\0';
            query.append(buffer, bytes_read);
            if (strchr(buffer, ';') != nullptr) break;
        }

        if (query.empty()) {
            // Client disconnected
            break;
        }

        while (!query.empty() && (query.back() == '\n' || query.back() == '\r')) {
            query.pop_back();
        }
        
        if (query.empty()) continue;

        std::string response;
        try {
            if (starts_with_keyword_ci(query, "INSERT")) {
                if (db.insert_into_raw_sql(query)) response = "OK\n<EOF>";
                else response = "Error: Insert failed\n<EOF>";
            } else {
            auto stmt = flexql::parser::Parser::parse(query);

            if (stmt.type == flexql::parser::StmtType::CREATE) {
                bool exists = !db.create_table(stmt, query);
                if (exists && stmt.create_if_not_exists) {
                    response = "OK\n<EOF>";
                } else if (exists) {
                    response = "Error: Table creation failed\n<EOF>";
                } else {
                    response = "OK\n<EOF>";
                }
            }
            else if (stmt.type == flexql::parser::StmtType::INSERT) {
                if (db.insert_into(stmt, query)) response = "OK\n<EOF>";
                else response = "Error: Insert failed\n<EOF>";
            }
            else if (stmt.type == flexql::parser::StmtType::DELETE) {
                if (db.delete_from(stmt, query)) response = "OK\n<EOF>";
                else response = "Error: Delete failed\n<EOF>";
            }
            else if (stmt.type == flexql::parser::StmtType::SELECT) {
                std::string res = db.select_from(stmt);
                if (res.find("Error:") == 0) {
                    // Send error with the <EOF> terminator
                    response = res + "<EOF>";
                } else {
                    // Append OK\n<EOF> exactly as the benchmark client expects 
                    response = res + "OK\n<EOF>";
                }
            }
            else {
                response = "Error: Unknown command\n<EOF>";
            }
            }
        } catch (const std::exception& e) {
            response = std::string("Error: ") + e.what() + "\n<EOF>";
        }

        // Send back securely 
        const char* ptr = response.c_str();
        size_t left = response.length();
        
        while (left > 0) {
            ssize_t sent = send(client_fd, ptr, left, 0);
            if (sent <= 0) break;
            ptr += sent;
            left -= sent;
        }
    }
    close(client_fd);
}

int main() {
    size_t num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 4;
    flexql::concurrency::ThreadPool pool(num_threads);
    
    // Auto-wipe the database on boot just like flexql_server.cpp does for the benchmark
    // std::remove("flexql.db"); // removed to avoid data wipe bug
    flexql::storage::Database db;
    flexql::network::TcpServer server("127.0.0.1", 9000);
    if (!server.start()) {
        std::cerr << "Failed to start server.\n";
        return 1;
    }
    
    std::cout << "FlexQL Database Server listening on 127.0.0.1:9000\n";

    while (true) {
        int client_fd = server.accept_connection();
        if (client_fd >= 0) {
            // std::cout << "[Server] Accepted new connection.\n";
            pool.enqueue([client_fd, &db]() { 
                handle_client(client_fd, db); 
            });
        }
    }

    server.stop();
    return 0;
}