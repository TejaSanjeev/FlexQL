#include "flexql.h"
#include <iostream>
#include <string>
#include <vector>
#include <cstring>
#include <cctype>
#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>

struct FlexQL {
    int sockfd = -1;
    std::string host;
    int port = 0;
};

namespace {
bool send_all(int sockfd, const char* data, size_t len) {
    size_t sent_total = 0;
    while (sent_total < len) {
        ssize_t sent = send(sockfd, data + sent_total, len - sent_total, 0);
        if (sent <= 0) return false;
        sent_total += static_cast<size_t>(sent);
    }
    return true;
}

int connect_socket(const std::string& host, int port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    std::string port_str = std::to_string(port);
    addrinfo* res = nullptr;
    if (getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0 || !res) {
        return -1;
    }

    int sock = -1;
    for (addrinfo* rp = res; rp != nullptr; rp = rp->ai_next) {
        int candidate = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (candidate < 0) continue;
        if (connect(candidate, rp->ai_addr, rp->ai_addrlen) == 0) {
            sock = candidate;
            break;
        }
        close(candidate);
    }

    freeaddrinfo(res);
    return sock;
}

bool reconnect(FlexQL* db) {
    if (!db) return false;

    if (db->sockfd >= 0) {
        shutdown(db->sockfd, SHUT_RDWR);
        close(db->sockfd);
        db->sockfd = -1;
    }

    int sock = connect_socket(db->host, db->port);
    if (sock < 0) return false;
    db->sockfd = sock;
    return true;
}

void set_error(char** errmsg, const std::string& msg) {
    if (errmsg) *errmsg = strdup(msg.c_str());
}

int parse_and_invoke_callback(const std::string& line,
                              int (*callback)(void*, int, char**, char**),
                              void* arg) {
    if (!callback || line.empty()) return 0;

    std::string data = line;
    std::vector<std::string> col_names;
    std::vector<std::string> col_vals;

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

        size_t eq_pos = token.find('=');
        if (eq_pos != std::string::npos) {
            col_names.push_back(token.substr(0, eq_pos));
            col_vals.push_back(token.substr(eq_pos + 1));
        }
    }

    int col_count = static_cast<int>(col_names.size());
    if (col_count == 0) return 0;

    std::vector<char*> c_names(col_count);
    std::vector<char*> c_vals(col_count);
    for (int i = 0; i < col_count; ++i) {
        c_names[i] = strdup(col_names[i].c_str());
        c_vals[i] = strdup(col_vals[i].c_str());
    }

    int rc = callback(arg, col_count, c_vals.data(), c_names.data());

    for (int i = 0; i < col_count; ++i) {
        free(c_names[i]);
        free(c_vals[i]);
    }

    return rc;
}

bool starts_with_error(const std::string& line) {
    return line.rfind("Error", 0) == 0;
}

bool is_select_sql(const char* sql) {
    if (!sql) return false;
    size_t i = 0;
    while (sql[i] != '\0' && std::isspace(static_cast<unsigned char>(sql[i]))) {
        ++i;
    }
    static const char* k = "SELECT";
    for (size_t j = 0; k[j] != '\0'; ++j) {
        if (sql[i + j] == '\0') return false;
        if (std::toupper(static_cast<unsigned char>(sql[i + j])) != k[j]) return false;
    }
    return true;
}
}

int flexql_open(const char *host, int port, FlexQL **db) {
    if (!host || !db || port <= 0 || port > 65535) return FLEXQL_ERROR;

    int sock = connect_socket(host, port);
    if (sock < 0) return FLEXQL_ERROR;

    *db = new FlexQL{};
    (*db)->sockfd = sock;
    (*db)->host = host;
    (*db)->port = port;
    return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
    if (!db) return FLEXQL_ERROR;

    if (db->sockfd >= 0) {
        shutdown(db->sockfd, SHUT_RDWR);
        close(db->sockfd);
    }
    delete db;
    return FLEXQL_OK;
}

void flexql_free(void *ptr) {
    if (ptr) free(ptr);
}

int flexql_exec(FlexQL *db, const char *sql, int (*callback)(void*, int, char**, char**), void *arg, char **errmsg) {
    if (!db || !sql) return FLEXQL_ERROR;

    if (db->sockfd < 0 && !reconnect(db)) {
        set_error(errmsg, "Connection is not available");
        return FLEXQL_ERROR;
    }

    if (!send_all(db->sockfd, sql, strlen(sql))) {
        set_error(errmsg, "Failed to send query");
        return FLEXQL_ERROR;
    }

    bool is_select = is_select_sql(sql);

    std::string pending;
    std::string info_output;
    char buffer[8192];

    auto process_line = [&](std::string line) -> int {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty()) return 0;

        if (starts_with_error(line)) {
            set_error(errmsg, line);
            return -1;
        }

        if (is_select) {
            if (line == "OK") return 0;
            if (!callback) return 0;

            int cb_rc = parse_and_invoke_callback(line, callback, arg);
            if (cb_rc != 0) {
                // Best-effort abort: terminate the socket so server send loop stops early.
                shutdown(db->sockfd, SHUT_RDWR);
                close(db->sockfd);
                db->sockfd = -1;

                if (!reconnect(db)) {
                    set_error(errmsg, "Query aborted by callback; reconnect failed");
                    return -1;
                }
                return 1;
            }
            return 0;
        }

        if (line != "OK") {
            if (!info_output.empty()) info_output.push_back('\n');
            info_output += line;
        }
        return 0;
    };

    while (true) {
        int bytes = recv(db->sockfd, buffer, sizeof(buffer), 0);
        if (bytes <= 0) {
            set_error(errmsg, "Connection lost");
            return FLEXQL_ERROR;
        }

        pending.append(buffer, bytes);
        size_t eof_pos = pending.find("<EOF>");

        if (eof_pos != std::string::npos) {
            std::string final_block = pending.substr(0, eof_pos);
            size_t start = 0;
            while (start <= final_block.size()) {
                size_t nl = final_block.find('\n', start);
                std::string line;
                if (nl == std::string::npos) {
                    line = final_block.substr(start);
                    start = final_block.size() + 1;
                } else {
                    line = final_block.substr(start, nl - start);
                    start = nl + 1;
                }

                int rc = process_line(std::move(line));
                if (rc == -1) return FLEXQL_ERROR;
                if (rc == 1) return FLEXQL_OK;
            }
            break;
        }

        size_t last_nl = pending.find_last_of('\n');
        if (last_nl == std::string::npos) {
            continue;
        }

        std::string block = pending.substr(0, last_nl + 1);
        pending.erase(0, last_nl + 1);

        size_t start = 0;
        while (start < block.size()) {
            size_t nl = block.find('\n', start);
            if (nl == std::string::npos) break;

            std::string line = block.substr(start, nl - start);
            start = nl + 1;

            int rc = process_line(std::move(line));
            if (rc == -1) return FLEXQL_ERROR;
            if (rc == 1) return FLEXQL_OK;
        }
    }

    if (!is_select && !info_output.empty()) {
        std::cout << info_output << "\n";
    }

    return FLEXQL_OK;
}
