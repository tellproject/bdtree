#include "mongo_impl.h"

#ifdef MongoDB_FOUND

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"
#pragma GCC diagnostic ignored "-Wshorten-64-to-32"
#pragma GCC diagnostic ignored "-Wstring-conversion"
#pragma GCC diagnostic ignored "-Wconversion"
#include <mongo/client/dbclient.h>
#pragma GCC diagnostic pop
#include <memory>

class mongo_client : public DB {
    mongo::DBClientConnection con;
public:
    mongo_client(const std::string& url) {
        con.connect(url);
    }

    int read(const std::string& table,
             const std::string& key,
             const std::unordered_set<std::string>& fields,
             std::unordered_map<std::string, std::string>& result)  override
    {
        //std::unique_ptr<mongo::DBClientCursor> cursor;
        return 0;
    }
    int scan(const std::string& table,
             const std::string& start_key,
             int read_count,
             const std::unordered_set<std::string>& fields,
             std::vector<std::unordered_map<std::string, std::string> >& result) override
    {
        return 0;
    }
    int update(const std::string& table,
               const std::string& key,
               const std::unordered_map<std::string, std::string>& values) override
    {
        return 0;
    }

    int insert(const std::string& table,
               const std::string& key,
               const std::unordered_map<std::string, std::string>& values) override
    {
        return 0;
    }
    int remove(const std::string& table,
               const std::string& key) override
    {
        return 0;
    }
};

DB* create_mongo_db(const std::string& url)
{
    return new mongo_client(url);
}

#endif
