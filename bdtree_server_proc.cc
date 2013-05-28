#include "bdtree_server_proc.h"
#include "bdtree.h"

#include <string>
#include <memory>
#include <iostream>
#include <unordered_set>
#include <stdint.h>
#include <cassert>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

namespace {

enum class server_op : char {
    Read = 1,
    Scan,
    Update,
    Insert,
    Delete,
    Close
};

struct tables {
    uint64_t data;
    bdtree::logical_pointer_table lpt;
    bdtree::node_table nt;
    tables(const std::string name) {
        auto res = rc_get_table_id(&data, name.c_str());
        if (res != STATUS_OK) {
            rc_create_table(name.c_str());
            rc_get_table_id(&data, name.c_str());
        }
        res = rc_get_table_id(&lpt.value, (name + "_lpt").c_str());
        if (res != STATUS_OK) {
            rc_create_table(name.c_str());
            rc_get_table_id(&data, name.c_str());
        }
        res = rc_get_table_id(&nt.value, (name + "_nt").c_str());
        if (res != STATUS_OK) {
            rc_create_table(name.c_str());
            rc_get_table_id(&data, name.c_str());
        }
    }
};

}

void server::run(int fd, DB& db)
{
    server s(fd, db);
    s.exec();
}

server::~server()
{
    close(fd);
}

/**
 * @brief server executes the server process
 * It first reads the operation and then executes the
 * according command.
 * @param fd The file descriptor of a client connections
 */
void server::exec() {
    for (;;) {
        server_op op;
        read(fd, &op, sizeof(op));
        switch (op) {
        case server_op::Read:
        case server_op::Scan:
        case server_op::Update:
        case server_op::Insert:
        case server_op::Delete:
        case server_op::Close:
            close(fd);
            return;
        default:
            assert(false);
            close(fd);
            return;
        }
    }
}

int32_t server::read_int() {
    int32_t res;
    read(fd, &res, sizeof(res));
    unsigned char b1, b2, b3, b4;

    b1 = res & 255;
    b2 = ( res >> 8 ) & 255;
    b3 = ( res>>16 ) & 255;
    b4 = ( res>>24 ) & 255;

    return ((int)b1 << 24) + ((int)b2 << 16) + ((int)b3 << 8) + b4;
}

// A string is 4 bytes big endian int length + the string
std::string server::read_string() {
    size_t len = size_t(read_int());
    std::unique_ptr<char[]> resp(new char[len]);
    size_t r = 0;
    while (r < len) {
        ssize_t res = read(fd, resp.get(), len - r);
        if (res < 0) {
            assert(false);
            std::cout << "ERROR: Could not read " << strerror(errno) << std::endl;
            exit(1);
        }
        r += size_t(res);
    }
    return std::string(resp.get());
}

std::unordered_set<std::string> server::read_fields() {
    std::unordered_set<std::string> res;
    int32_t size = read_int();
    for (int32_t i = 0; i < size; ++i) {
        res.insert(read_string());
    }
    return res;
}

void server::db_read() {
    // 1. read the table name
    std::string tname = read_string();
    // open the tables
    tables t(tname);
    // 2. Get the key
    std::string key = read_string();
    std::unordered_set<std::string> fields = read_fields();
}
