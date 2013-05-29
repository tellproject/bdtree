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
#include <sys/socket.h>
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
            db_read();
            break;
        case server_op::Scan:
            db_scan();
            break;
        case server_op::Update:
            db_update();
            break;
        case server_op::Insert:
            db_insert();
            break;
        case server_op::Delete:
            db_delete();
            break;
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

void server::db_read() {
    // 1. read the table name
    std::string tname = read_string();
    // 2. Get the key
    std::string key = read_string();
    // 3. Get the fields
    std::unordered_set<std::string> fields = read_fields();
    std::unordered_map<std::string, std::string> res;
    int32_t err = this->db.read(tname, key, fields, res);
    send_int(err);
    send_map(res);
}

void server::db_scan()
{
    // 1. read the table name
    std::string tname = read_string();
    // 2. Get the start key
    std::string key = read_string();
    // 3. read the record count
    int32_t reccount = read_int();
    // 4. read the fields
    std::unordered_set<std::string> fields = read_fields();
    std::vector<std::unordered_map<std::string, std::string> > result;
    int32_t err = db.scan(tname, key, reccount, fields, result);
    send_int(err);
    send_vec(result);
}

void server::db_update()
{
    auto table = read_string();
    auto key = read_string();
    auto values = read_map();
    auto err = db.update(table, key, values);
    send_int(err);
}

void server::db_insert()
{
    auto table = read_string();
    auto key = read_string();
    auto values = read_map();
    auto err = db.insert(table, key, values);
    send_int(err);
}

void server::db_delete()
{
    auto table = read_string();
    auto key = read_string();
    auto err = db.remove(table, key);
    send_int(err);
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
int32_t server::read_int() {
    int32_t res;
    ssize_t r = 0;
    while (r < sizeof(int32_t)) {
        r += read(fd, reinterpret_cast<char*>(&res) + r, sizeof(res) - r);
    }
    unsigned char b1, b2, b3, b4;

    b1 = res & 255;
    b2 = ( res >> 8 ) & 255;
    b3 = ( res>>16 ) & 255;
    b4 = ( res>>24 ) & 255;

    return ((int)b1 << 24) + ((int)b2 << 16) + ((int)b3 << 8) + b4;
}
#pragma GCC diagnostic push

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

std::unordered_map<std::string, std::string> server::read_map()
{
    std::unordered_map<std::string, std::string> res;
    int32_t size = read_int();
    for (int32_t i = 0; i < size; ++i) {
        res.insert(std::make_pair(read_string(), read_string()));
    }
    return res;
}

void server::send_map(const std::unordered_map<std::string, std::string> &m)
{
    int32_t num_elems = int32_t(m.size());
    send_int(num_elems);
    for (const std::pair<std::string, std::string>& p : m) {
        send_string(p.first);
        send_string(p.second);
    }
}

void server::send_vec(std::vector<std::unordered_map<std::string, std::string> > &v)
{
    int32_t num_elems = int32_t(v.size());
    send_int(num_elems);
    for (const auto& m : v) {
        send_map(m);
    }
}

void server::send_string(const std::string &s)
{
    int32_t size = int32_t(s.size());
    send_int(size);
    const char* arr = s.c_str();
    ssize_t r = 0;
    while (r < size) {
        r += send(fd, arr + r, size - r, 0);
    }
}

void server::send_int(int32_t i)
{
    unsigned char b1, b2, b3, b4;

    b1 = i & 255;
    b2 = ( i >> 8 ) & 255;
    b3 = ( i >>16 ) & 255;
    b4 = ( i >>24 ) & 255;

    int32_t to_send = ((int32_t)b1 << 24) + ((int32_t)b2 << 16) + ((int32_t)b3 << 8) + b4;
    ssize_t r = 0;
    while (r < sizeof(int32_t)) {
        r += ::send(fd, reinterpret_cast<char*>(&to_send) + r, sizeof(to_send) - r, 0);
    }
}
