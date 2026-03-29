#include "flexql.h"
#include <iostream>
#include <string>
#include <vector>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

struct FlexQL {
    int sockfd;
};

int flexql_open(const char *host, int port, FlexQL **db) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return FLEXQL_ERROR;

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &serv_addr.sin_addr) <= 0) return FLEXQL_ERROR;

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) return FLEXQL_ERROR;

    *db = new FlexQL{sock};
    return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
    if (db) {
        close(db->sockfd);
        delete db;
    }
    return FLEXQL_OK;
}

void flexql_free(void *ptr) {
    if (ptr) free(ptr);
}

int flexql_exec(FlexQL *db, const char *sql, int (*callback)(void*, int, char**, char**), void *arg, char **errmsg) {
    if (!db || !sql) return FLEXQL_ERROR;

    if (send(db->sockfd, sql, strlen(sql), 0) < 0) {
        if (errmsg) *errmsg = strdup("Failed to send query");
        return FLEXQL_ERROR;
    }

    // PHASE 2 FIX: Robust loop to read massive payloads until <EOF>
    std::string response;
    char buffer[8192];
    
    while (true) {
        memset(buffer, 0, sizeof(buffer));
        int bytes = recv(db->sockfd, buffer, sizeof(buffer) - 1, 0);
        
        if (bytes <= 0) {
            if (errmsg && response.empty()) *errmsg = strdup("Connection lost");
            break;
        }
        
        response += buffer;
        size_t eof_pos = response.find("<EOF>");
        if (eof_pos != std::string::npos) {
            response.erase(eof_pos); // Strip the marker out
            break;
        }
    }

    // Handle explicit errors
    if (response.find("Error") == 0) {
        if (errmsg) *errmsg = strdup(response.c_str());
        return FLEXQL_ERROR;
    }

    // Handle data payloads
    if (response.find("Row ") == 0 || response.find("Found: ") == 0) {
        if (!callback) return FLEXQL_OK;

        std::string delimiter = "\n";
        size_t pos = 0;
        std::string line;
        
        while ((pos = response.find(delimiter)) != std::string::npos || !response.empty()) {
            if (pos != std::string::npos) {
                line = response.substr(0, pos);
                response.erase(0, pos + delimiter.length());
            } else {
                line = response;
                response.clear();
            }

            if (line.empty()) continue;
            
            size_t start = line.find(':');
            if (start == std::string::npos) continue;
            std::string data = line.substr(start + 2); 
            
            std::vector<std::string> colNames;
            std::vector<std::string> colVals;
            
            // FIX 3: Bulletproof Tab-based parsing
            while (!data.empty()) {
                size_t token_pos = data.find('\t');
                std::string token;
                
                if (token_pos != std::string::npos) {
                    token = data.substr(0, token_pos);
                    data.erase(0, token_pos + 1);
                } else {
                    token = data;
                    data.clear();
                }

                if (token.empty()) continue;

                size_t eq_pos = token.find('='); // Find the FIRST equals sign
                if (eq_pos != std::string::npos) {
                    colNames.push_back(token.substr(0, eq_pos));
                    colVals.push_back(token.substr(eq_pos + 1)); // Everything after is safely the value
                }
            }
            
            int colCount = colNames.size();
            if (colCount > 0) {
                std::vector<char*> c_names(colCount);
                std::vector<char*> c_vals(colCount);

                for (int i = 0; i < colCount; i++) {
                    c_names[i] = strdup(colNames[i].c_str());
                    c_vals[i] = strdup(colVals[i].c_str());
                }

                // Actually pass multiple columns instead of argc=1 hack
                callback(arg, colCount, c_vals.data(), c_names.data());

                for (int i = 0; i < colCount; i++) {
                    free(c_names[i]);
                    free(c_vals[i]);
                }
            }
        }
    } else {
        // Handle informational messages cleanly ("Table created successfully", "No rows found.")
        while (!response.empty() && (response.back() == '\n' || response.back() == '\r')) {
            response.pop_back();
        }
        if (response != "OK") {
            std::cout << response << "\n";
        }
    }

    return FLEXQL_OK;
}