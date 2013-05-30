#include "bdtree_db_impl.h"
#include "bdtree.h"

#include <server_config.h>
#include <concurrent_map.h>
#include <serializer.h>

template<typename Archiver, typename T, typename V, typename Hash, typename KeyEqual, typename Allocator>
struct serialize_policy<Archiver, std::unordered_map<T, V, Hash, KeyEqual, Allocator> >
{
    uint8_t* operator() (Archiver& ar, const std::unordered_map<T, V, Hash, KeyEqual, Allocator>& v, uint8_t* pos) const {
        std::size_t s = v.size();
        ar & s;
        for (auto& e : v) {
            ar & e.first;
            ar & e.second;
        }
        return ar.pos;
    }
};

template<typename Archiver, typename T, typename V, typename Hash, typename KeyEqual, typename Allocator>
struct deserialize_policy<Archiver, std::unordered_map<T, V, Hash, KeyEqual, Allocator> >
{
    const uint8_t* operator() (Archiver& ar, std::unordered_map<T, V, Hash, KeyEqual, Allocator>& out, const uint8_t* ptr) const {
        const std::size_t s = *reinterpret_cast<const std::size_t*>(ptr);
        ar.pos = ptr + sizeof(s);
        for (std::size_t i = 0; i < s; ++i) {
            T f;
            V s;
            ar & f;
            ar & s;
            out.emplace(std::move(f), std::move(s));
        }
        return ar.pos;
    }
};

template<typename Archiver, typename T, typename V, typename Hash, typename KeyEqual, typename Allocator>
struct size_policy<Archiver, std::unordered_map<T, V, Hash, KeyEqual, Allocator>>
{
    std::size_t operator() (Archiver& ar, const std::unordered_map<T, V, Hash, KeyEqual, Allocator>& obj) const
    {
        std::size_t s;
        ar & s;
        for (auto& e : obj) {
            ar & e.first;
            ar & e.second;
        }
        return 0;
    }
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

class bdtree_client : public DB {
    bdtree::logical_table_cache<std::string, bdtree::empty_t>& cache;
    uint64_t data;
public:
    bdtree_client(bdtree::logical_table_cache<std::string, bdtree::empty_t>& c, uint64_t d) : cache(c), data(d) {}
    ~bdtree_client() {}
    int read(const std::string& table,
             const std::string& key,
             const std::unordered_set<std::string>& fields,
             std::unordered_map<std::string, std::string>& result)  override
    {
        awesome::allocator _;
        uint64_t tx = bdtree::get_next_tx_id();
#ifdef BDTREE_FOR_POINT_QUERY
        bdtree::map<std::string, bdtree::empty_t> map(cache, tx);
        auto iter = map.find(key);
        if (iter == map.end() || *iter != key) {
            assert(false);
            return 1;
        }
#endif
        ramcloud_buffer_t buf;
        auto rc_res = rc_read(data, key.c_str(), uint16_t(key.size()), &buf);
        assert(rc_res);
        std::unordered_map<std::string, std::string> res;
        deserialize(res, buf.data);
        for (const auto& f : fields) {
            result.insert(std::make_pair(f, res[f]));
        }
        return 0;
    }
    int scan(const std::string& table,
             const std::string& start_key,
             int read_count,
             const std::unordered_set<std::string>& fields,
             std::vector<std::unordered_map<std::string, std::string> >& result) override
    {
        awesome::allocator _;
        uint64_t tx = bdtree::get_next_tx_id();
        bdtree::map<std::string, bdtree::empty_t> map(cache, tx);
        auto iter = map.find(start_key);
        if (iter == map.end()) {
            assert(false);
            return 1;
        }
        std::vector<std::string> keys;
        keys.reserve(size_t(read_count));
        auto end = map.end();
        for (auto i = 0; i < read_count && iter != end; ++i) {
            keys.push_back(*iter);
            ++iter;
        }
        multi_read_request req(uint32_t(keys.size()));
        for (uint32_t i = 0; i < keys.size(); ++i) {
            req.push_back(data, keys[i].c_str(), uint16_t(keys[i].size()));
        }
        rc_multi_read(req);
        result.reserve(keys.size());
        for (uint32_t i = 0; i < keys.size(); ++i) {
            auto& rbuf = req[i].rbuf;
            auto length = rbuf->getTotalLength();
            std::unique_ptr<uint8_t[]> buf(new uint8_t[length]);
            req[i].rbuf->copy(0, length, buf.get());
            std::unordered_map<std::string, std::string> resmap;
            deserialize(resmap, buf.get());
            result.emplace_back(std::move(resmap));
        }
        return 0;
    }
    int update(const std::string& table,
               const std::string& key,
               const std::unordered_map<std::string, std::string>& values) override
    {
        awesome::allocator _;
        uint64_t tx = bdtree::get_next_tx_id();
#ifdef BDTREE_FOR_POINT_QUERY
        bdtree::map<std::string, bdtree::empty_t> map(cache, tx);
        auto iter = map.find(key);
        if (iter == map.end() || *iter != key) {
            assert(false);
            return 1;
        }
#endif
        ramcloud_buffer_t buf;
        auto rc_res = rc_read(data, key.c_str(), uint16_t(key.size()), &buf);
        assert(rc_res);
        std::unordered_map<std::string, std::string> res;
        deserialize(res, buf.data);
        for (const auto& p : values) {
            res[p.first] = p.second;
        }
        std::unique_ptr<uint8_t[]> serres;
        auto sersize = serialize(serres, res);
        rc_res = rc_write(data, key.c_str(), uint16_t(key.size()), reinterpret_cast<const char*>(serres.get()), uint32_t(sersize));
        assert(rc_res == STATUS_OK);
        return 0;
    }

    int insert(const std::string& table,
               const std::string& key,
               const std::unordered_map<std::string, std::string>& values) override
    {
        awesome::allocator _;
        uint64_t tx = bdtree::get_next_tx_id();
        std::unique_ptr<uint8_t[]> serres;
        auto sersize = serialize(serres, values);
        auto rc_res = rc_write(data, key.c_str(), uint16_t(key.size()), reinterpret_cast<const char*>(serres.get()), uint32_t(sersize));
        assert(rc_res == STATUS_OK);
        bdtree::map<std::string, bdtree::empty_t> map(cache, tx);
        auto inserted = map.insert(key, bdtree::empty_t());
        assert(inserted);
        return 0;
    }
    int remove(const std::string& table,
               const std::string& key) override
    {
        awesome::allocator _;
        uint64_t tx = bdtree::get_next_tx_id();
        bdtree::map<std::string, bdtree::empty_t> map(cache, tx);
        auto removed = map.erase(key);
        assert(removed);
        auto rc_res = rc_remove(data, key.c_str(), uint16_t(key.size()));
        assert(rc_res == STATUS_OK);
        return 0;
    }
};

DB* create_bdtree_db(bdtree::logical_table_cache<std::string, bdtree::empty_t>& cache, uint64_t data)
{
    return new bdtree_client(cache, data);
}
