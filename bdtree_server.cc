#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <errmsg.h>
#include <signal.h>
#include <string>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "amalloc.h"
#include "bdtree_server_proc.h"
#include "cramcloud.h"
#include "bdtree_db_impl.h"
#include "mongo_impl.h"

namespace {
int server_socket = 0;

void on_exit(int sig) {
    if (server_socket) {
        close(server_socket);
    }
    exit(0);
}
}

void printhelp() {
    std::cout << "Usage: " << std::endl;
    std::cout << '\t' << "-h         Print this help message" << std::endl;
    std::cout << '\t' << "-H host    Bind server to host (default = nullptr)" << std::endl;
    std::cout << '\t' << "-p port    Bind server to port" << std::endl;
    std::cout << '\t' << "-C host    Coordinator host (use RamCloud)" << std::endl;
    std::cout << '\t' << "-M host    MongoDB host (use MongoDB)" << std::endl;
}

struct addr_freeer {
    void operator() (addrinfo* i) {
        freeaddrinfo(i);
    }
};


struct sock_closer {
    int sock;
    ~sock_closer() {
        close(sock);
    }
};

uint8_t* ralloc(size_t s) {
    return new uint8_t[s];
}

void rdealloc(uint8_t* ptr) {
    delete[] ptr;
}

int main(int argc, char* argv[]) {
    awesome::init();
    signal(SIGTERM, &on_exit);
    signal(SIGKILL, &on_exit);
    signal(SIGHUP, &on_exit);
    signal(SIGQUIT, &on_exit);
    int ch;
    bool use_mongo = false;
    std::string port = "8706";
    std::string host, dbhost;
    while ((ch = getopt(argc, argv, "hH:C:p:M:")) != -1) {
        switch (ch) {
        case 'h':
            printhelp();
            return 0;
        case 'H':
            host = optarg;
            break;
        case  'C':
            dbhost = optarg;
            break;
        case 'p':
            port = optarg;
            break;
        case 'M':
            use_mongo = true;
            dbhost = optarg;
            break;
        default:
            printhelp();
            return 1;
        }
    }
    std::unique_ptr<DB> connection;
    if (use_mongo) {
        connection.reset(create_mongo_db(dbhost));
    } else {
        init_ram_cloud(dbhost.c_str(), &ralloc, &rdealloc);
        connection.reset(create_bdtree_db());
    }
    const char* hname = host == "" ? nullptr : host.c_str();
    std::unique_ptr<addrinfo, addr_freeer> ainfop;
    addrinfo* ainfo;
    {
        int err = getaddrinfo(hname, port.c_str(), nullptr, &ainfo);
        if (!err) {
            std::cerr << gai_strerror(err) << std::endl;
            return 1;
        }
        ainfop.reset(ainfo);
    }
    server_socket = socket(ainfo->ai_family, ainfo->ai_socktype, ainfo->ai_protocol);
    if (server_socket < 0) {
        std::cerr << "Could not create socket" << std::endl;
        return 1;
    }
    int err = bind(server_socket, ainfo->ai_addr, ainfo->ai_addrlen);
    if (!err) {
        std::cerr << "bind failed: " << strerror(errno) << std::endl;
        return 1;
    }
    err = listen(server_socket, 5);
    if (!err) {
        std::cerr << "listen failed: " << strerror(errno) << std::endl;
        return 1;
    }
    std::vector<std::thread> threads;
    for (;;) {
        sockaddr cliaddr;
        socklen_t cliaddrlen;
        int fd = accept(server_socket, &cliaddr, &cliaddrlen);
        if (fd < 0) {
            std::cerr << "Accept failed: " << strerror(errno);
            continue;
        }
        threads.push_back(std::thread([fd, &connection](){server::run(fd, *connection);}));
    }
    return 0;
}
