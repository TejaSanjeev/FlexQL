#include "network/tcp_server.h"
#include <iostream>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#if defined(__linux__) || defined(__APPLE__)
#include <netinet/tcp.h>
#endif
#include <cstring>

namespace flexql {
namespace network {

TcpServer::TcpServer(const std::string& host, int port) 
    : host_(host), port_(port), server_fd_(-1), is_running_(false) {
    memset(&address_, 0, sizeof(address_));
}

TcpServer::~TcpServer() {
    stop();
}

bool TcpServer::start() {
    // 1. Create socket file descriptor (IPv4, TCP)
    if ((server_fd_ = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        std::cerr << "Socket creation failed.\n";
        return false;
    }

    // 2. Set socket options for aggressive port reuse
    int opt = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        std::cerr << "Setsockopt failed.\n";
        return false;
    }

#ifdef SO_REUSEPORT
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif

#ifdef TCP_NODELAY
    int nodelay = 1;
    setsockopt(server_fd_, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
#endif

    // 3. Bind socket to IP and Port
    address_.sin_family = AF_INET;
    address_.sin_port = htons(port_);
    if (inet_pton(AF_INET, host_.c_str(), &address_.sin_addr) <= 0) {
        std::cerr << "Invalid address / Address not supported: " << host_ << "\n";
        return false;
    }

    if (bind(server_fd_, (struct sockaddr*)&address_, sizeof(address_)) < 0) {
        std::cerr << "Bind failed on " << host_ << ":" << port_ << "\n";
        return false;
    }

    // 4. Start listening (backlog of 128 pending connections)
    if (listen(server_fd_, 128) < 0) {
        std::cerr << "Listen failed.\n";
        return false;
    }

    is_running_ = true;
    return true;
}

int TcpServer::accept_connection() {
    if (!is_running_) return -1;

    int addrlen = sizeof(address_);
    int client_fd = accept(server_fd_, (struct sockaddr*)&address_, (socklen_t*)&addrlen);
    
    if (client_fd < 0) {
        std::cerr << "Failed to accept connection.\n";
    } else {
#ifdef TCP_NODELAY
        int nodelay = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
#endif
    }
    return client_fd;
}

void TcpServer::stop() {
    if (is_running_ && server_fd_ != -1) {
        close(server_fd_);
        server_fd_ = -1;
        is_running_ = false;
    }
}

} // namespace network
} // namespace flexql