#include "mongo_impl.h"

#ifdef MongoDB_FOUND

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"
//#pragma GCC diagnostic ignored "-Wshorten-64-to-32"
//#pragma GCC diagnostic ignored "-Wstring-conversion"
#pragma GCC diagnostic ignored "-Wconversion"
#include <mongo/client/dbclient.h>
#pragma GCC diagnostic pop
#include <memory>

using namespace mongo;

class mongo_client : public DB {
    DBClientConnection con;
public:
    mongo_client(const std::string& url) {
        con.connect(url);
    }

    ~mongo_client() {}

    int read(const std::string& table,
             const std::string& key,
             const std::unordered_set<std::string>& fields,
             std::unordered_map<std::string, std::string>& result)  override
    {
        std::unique_ptr<DBClientCursor> cursor = con.query("YCSB.usertable", QUERY("_id" << key));
        if (!cursor->more()) {
            assert(false);
            return 1;
        }
        BSONObj p = cursor->next();
        if (fields.empty()) {
            std::set<std::string> fields;
            p.getFieldNames(fields);
            for (auto& f : fields) {
                result.emplace(f, p.getField(f).str());
            }
        } else {
            for (const auto& f : fields) {
                result.emplace(f, p.getField(f).str());
            }
        }
        return 0;
    }
    int scan(const std::string& table,
             const std::string& start_key,
             int read_count,
             const std::unordered_set<std::string>& fields,
             std::vector<std::unordered_map<std::string, std::string> >& result) override
    {
        BSONObj scan_range = BSON("$gte" << start_key);
        std::unique_ptr<DBClientCursor> cursor = con.query("YCSB.usertable", QUERY("_id" << scan_range), read_count);
        assert(cursor.get());
        while (cursor->more()) {
            std::unordered_map<std::string, std::string> resmap;
            BSONObj p = cursor->next();
            if (fields.empty()) {
                std::set<std::string> fields;
                p.getFieldNames(fields);
                for (auto& f : fields) {
                    resmap.emplace(f, p.getField(f).str());
                }
            } else {
                for (const auto& f : fields) {
                    resmap.emplace(f, p.getField(f).str());
                }
            }
            result.emplace_back(std::move(resmap));
        }
        return 0;
    }
    int update(const std::string& table,
               const std::string& key,
               const std::unordered_map<std::string, std::string>& values) override
    {
        BSONObjBuilder builder;
        for (const auto& p : values) {
            builder.append(p.first, p.second);
        }
        con.update("YCSB.usertable", QUERY("_id" << key), builder.obj());
        return 0;
    }

    int insert(const std::string& table,
               const std::string& key,
               const std::unordered_map<std::string, std::string>& values) override
    {
        BSONObjBuilder builder;
        builder.append("_id", key);
        for (const auto& p : values) {
            builder.append(p.first, p.second);
        }
        con.insert("YCSB.usertable", builder.obj());
        return 0;
    }
    int remove(const std::string& table,
               const std::string& key) override
    {
        con.remove("YCSB.usertable", QUERY("_id" << key));
        return 0;
    }
};

DB* create_mongo_db(const std::string& url)
{
    return new mongo_client(url);
}

#endif
