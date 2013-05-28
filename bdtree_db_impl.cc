#include "bdtree_db_impl.h"

class bdtree_client : public DB {
public:
    int read(const std::string& table,
             const std::string& key,
             const std::unordered_set<std::string>& fields,
             std::unordered_map<std::string, std::string>& result)  override
    {
        return 0;
    }
    int scan(const std::string& table,
             const std::string& start_key,
             int read_count,
             const std::unordered_set<std::string>& fields,
             std::unordered_map<std::string, std::string>& result) override
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

DB* create_bdtree_db()
{
    return new bdtree_client();
}
