#pragma once
#include <stdint.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>

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
                     std::vector<std::unordered_map<std::string, std::string> >& result) = 0;
    virtual int update(const std::string& table,
                       const std::string& key,
                       const std::unordered_map<std::string, std::string>& values) = 0;
    virtual int insert(const std::string& table,
                       const std::string& key,
                       const std::unordered_map<std::string, std::string>& values) = 0;
    virtual int remove(const std::string& table,
                       const std::string& key) = 0;
};

class connection;

class server {
    DB& db;
    std::unique_ptr<connection> con;
    server(int fd, DB& db);
    void exec();
    void db_read();
    void db_scan();
    void db_update();
    void db_insert();
    void db_delete();
public:
    static void run(int fd, DB* db);
    ~server();
};
