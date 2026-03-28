#ifndef FLEXQL_TCP_SERVER_H
#define FLEXQL_TCP_SERVER_H

#include <string>
#include <netinet/in.h>

namespace flexql {
namespace network {

class TcpServer {
public:
    TcpServer(const std::string& host, int port);
    ~TcpServer();

    // Initializes the socket, binds, and starts listening
    bool start();
    
    // Blocks and waits for a new client. Returns the client socket FD.
    int accept_connection();
    
    // Shuts down the server socket
    void stop();

private:
    std::string host_;
    int port_;
    int server_fd_;
    struct sockaddr_in address_;
    bool is_running_;
};

} // namespace network
} // namespace flexql

#endif // FLEXQL_TCP_SERVER_H