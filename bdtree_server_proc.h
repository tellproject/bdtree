#pragma once
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <unordered_set>

class DB {
public:
    virtual ~DB() {}
    virtual int read(const std::string& table,
                     const std::string& key,
                     const std::unordered_set<std::string>& fields,
                     std::unordered_map<std::string, std::string>& result) = 0;
    virtual int scan(const std::string& table,
                     const std::string& start_key,
                     int read_count,
                     const std::unordered_set<std::string>& fields,
                     std::unordered_map<std::string, std::string>& result) = 0;
    virtual int update(const std::string& table,
                       const std::string& key,
                       const std::unordered_map<std::string, std::string>& values) = 0;
    virtual int insert(const std::string& table,
                       const std::string& key,
                       const std::unordered_map<std::string, std::string>& values) = 0;
    virtual int remove(const std::string& table,
                       const std::string& key) = 0;
};

class server {
    int fd;
    DB& db;
    server(int fd, DB& db) : fd(fd), db(db) {}
    void exec();
    int32_t read_int();
    std::string read_string();
    std::unordered_set<std::string> read_fields();
    void db_read();
public:
    static void run(int fd, DB& db);
    ~server();
};
