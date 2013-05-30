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

class connection {
    int fd;
    uint8_t buf[2048];
    uint8_t* pos;
    uint8_t* rpos;
    uint8_t* end;
    uint8_t send_buf[2048];
    uint8_t* spos;
public:
    connection(int fd)
        : fd(fd), pos(buf), rpos(buf), end(buf + 2048), spos(send_buf) {}
    ~connection() { close(fd); }

    bool fill_buf(ssize_t size)
    {
        // read everything in buffer
        if (rpos == pos) {
            pos = buf;
            rpos = pos;
        }
        while (rpos - pos < size) {
            ssize_t r = recv(fd, pos, end - pos, 0);
            if (r == 0)
                return false;
            rpos += r;
        }
        return true;
    }

    bool get_op(server_op& op) {
        if (!fill_buf(1)) {
            return false;
        }
        op = *reinterpret_cast<server_op*>(pos++);
        return true;
    }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
    int32_t read_int() {
        fill_buf(sizeof(int32_t));
        int32_t res = *reinterpret_cast<int32_t*>(pos);
        pos += sizeof(int32_t);
        unsigned char b1, b2, b3, b4;

        b1 = res & 255;
        b2 = ( res >> 8 ) & 255;
        b3 = ( res>>16 ) & 255;
        b4 = ( res>>24 ) & 255;

        return ((int)b1 << 24) + ((int)b2 << 16) + ((int)b3 << 8) + b4;
    }
#pragma GCC diagnostic push

    // A string is 4 bytes big endian int length + the string
    std::string read_string() {
        size_t len = size_t(read_int());
        fill_buf(ssize_t(len));
        auto s = pos;
        pos += len;
        return std::string(reinterpret_cast<const char*>(s), len);
    }

    std::unordered_set<std::string> read_fields() {
        std::unordered_set<std::string> res;
        int32_t size = read_int();
        for (int32_t i = 0; i < size; ++i) {
            res.insert(read_string());
        }
        return res;
    }

    std::unordered_map<std::string, std::string> read_map()
    {
        std::unordered_map<std::string, std::string> res;
        int32_t size = read_int();
        for (int32_t i = 0; i < size; ++i) {
            auto key = read_string();
            auto value = read_string();
            res.insert(std::make_pair(key, value));
        }
        return res;
    }

    void flush() {
        uint8_t* s = send_buf;
        while (s != spos) {
            auto r = send(fd, s, spos - s, 0);
            s += r;
        }
        spos = send_buf;
    }

    void send_int(int32_t i)
    {
        unsigned char b1, b2, b3, b4;

        b1 = i & 255;
        b2 = ( i >> 8 ) & 255;
        b3 = ( i >>16 ) & 255;
        b4 = ( i >>24 ) & 255;

        int32_t to_send = ((int32_t)b1 << 24) + ((int32_t)b2 << 16) + ((int32_t)b3 << 8) + b4;
        if (spos - send_buf <= 2048 - sizeof(int32_t)) flush();
        memcpy(spos, &to_send, sizeof(int32_t));
        spos += sizeof(int32_t);
    }

    void send_string(const std::string &s)
    {
        int32_t size = int32_t(s.size());
        send_int(size);
        if (spos - send_buf <= 2048 - size)
            flush();
        const char* arr = s.c_str();
        if (size > 2048) {
            ssize_t r = 0;
            while (r < size) {
                r += send(fd, arr + r, size - r, 0);
            }
        } else {
            memcpy(spos, arr, size);
            spos += size;
        }
    }

    void send_map(const std::unordered_map<std::string, std::string> &m)
    {
        int32_t num_elems = int32_t(m.size());
        send_int(num_elems);
        for (const std::pair<std::string, std::string>& p : m) {
            send_string(p.first);
            send_string(p.second);
        }
    }

    void send_vec(std::vector<std::unordered_map<std::string, std::string> > &v)
    {
        int32_t num_elems = int32_t(v.size());
        send_int(num_elems);
        for (const auto& m : v) {
            send_map(m);
        }
    }
};

server::server(int fd, DB& db) : db(db), con(new connection(fd)) {}

void server::run(int fd, DB* db)
{
    server s(fd, *db);
    s.exec();
    delete db;
}

server::~server()
{
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
        if (!con->get_op(op))
            return;
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
            return;
        default:
            assert(false);
            return;
        }
    }
}

void server::db_read() {
    // 1. read the table name
    std::string tname = con->read_string();
    // 2. Get the key
    std::string key = con->read_string();
    // 3. Get the fields
    std::unordered_set<std::string> fields = con->read_fields();
    std::unordered_map<std::string, std::string> res;
    int32_t err = this->db.read(tname, key, fields, res);
    con->send_int(err);
    con->send_map(res);
    con->flush();
}

void server::db_scan()
{
    // 1. read the table name
    std::string tname = con->read_string();
    // 2. Get the start key
    std::string key = con->read_string();
    // 3. read the record count
    int32_t reccount = con->read_int();
    // 4. read the fields
    std::unordered_set<std::string> fields = con->read_fields();
    std::vector<std::unordered_map<std::string, std::string> > result;
    int32_t err = db.scan(tname, key, reccount, fields, result);
    con->send_int(err);
    con->send_vec(result);
    con->flush();
}

void server::db_update()
{
    auto table = con->read_string();
    auto key = con->read_string();
    auto values = con->read_map();
    auto err = db.update(table, key, values);
    con->send_int(err);
    con->flush();
}

void server::db_insert()
{
    auto table = con->read_string();
    auto key = con->read_string();
    auto values = con->read_map();
    auto err = db.insert(table, key, values);
    con->send_int(err);
    con->flush();
}

void server::db_delete()
{
    auto table = con->read_string();
    auto key = con->read_string();
    auto err = db.remove(table, key);
    con->send_int(err);
    con->flush();
}

