#include "cramcloud.h"

#include <thread>
#include <mutex>
#include <functional>
#include <array>
#include <chrono>
#include <cassert>
#include <cstdio>
#include <boost/functional/hash.hpp>
#include <unordered_map>
#include <atomic>
#include <random>

#include <singleconsumerqueue.h>
#include <concurrent_map.h>
#include <singleton.h>
#include "stacktrace.h"



const uint32_t max_len = 0x100000;

//std::size_t hash_value(const id_class &b)
//{
//    btree::buffer buf = b;
//    return boost::hash_range(data, data + buf.length());
//}


std::atomic_uint thread_id_counter;
std::mt19937 random_gen;
std::uniform_int_distribution<uint32_t> int_dist;
__thread uint thread_id;

struct ThreadInfo {
    ThreadInfo() {lock = 0;done = false;}
    std::atomic_uint lock;
    std::atomic<bool> done;
};

uint thread_num = 0;
ThreadInfo* thread_locks = nullptr;

void rc_init_rand(uint32_t seed, uint32_t number_threads) {
    thread_num = number_threads;
    thread_id_counter = 0;
    random_gen.seed(seed);
    int_dist = decltype(int_dist)(0,number_threads-1);
    thread_locks = new ThreadInfo[number_threads];
}

uint32_t rc_get_thread_id() {
    thread_id = thread_id_counter++;
    return thread_id;
}

void rc_mark_done(uint32_t thread_id) {
    thread_locks[thread_id].done = true;
    thread_locks[thread_id].lock = 0;
}

void rc_run_rand() {
    uint no_lock_sequence = 0;
    auto r = int_dist(random_gen);
    for (;;) {
        if (!thread_locks[r].done) {
            thread_locks[r].lock = 1;
            no_lock_sequence = 0;
        }

        ++no_lock_sequence;
        auto next_r = int_dist(random_gen);
        if (no_lock_sequence > 10000) {
            //check if we are done
            for (uint i=0;i < thread_num; ++i) {
                if (!thread_locks[r].done)
                    no_lock_sequence = 0;
            }
            if (no_lock_sequence)
                break;//we are done
        }

        while (thread_locks[r].lock && !thread_locks[r].done);// wait for completion of work
        r = next_r;
    }
    delete [] thread_locks;
    thread_locks = nullptr;
}

struct ScopeLock {
    ~ScopeLock() {
        if (thread_locks)
            thread_locks[thread_id].lock = 0;
    }
};

ScopeLock current_thread_wait_for_turn() {
    if (thread_locks == nullptr)
        return ScopeLock();
    while (!thread_locks[thread_id].lock);
    return ScopeLock();
}


namespace std {

template<typename S, typename T>
struct hash<pair<S, T> >
{
    inline size_t operator()(const pair<S, T>& v) const
    {
        size_t h = hash_value(v.first);
        boost::hash_combine(h, v.second);
        return h;
    }
};

}


class rc_data {
    friend struct util::create_using_new<rc_data>;
public:
    struct Entry {
        const uint8_t* data() const {
            return _data.data();
        }
        uint32_t size() const{
            return uint32_t(_data.size());
        }
        uint64_t version = 0;
        std::vector<uint8_t> _data;
    };

    typedef util::concurrent_map<std::string, Entry > SingleTable;
public:
    alloc_ramcloud_buffer_t alloc_ = nullptr;
    dealloc_ramcloud_buffer_t dealloc_ = nullptr;
    util::concurrent_map<std::string, uint64_t> _name_map;
    util::concurrent_map<uint64_t,  std::shared_ptr<SingleTable> > _data;
    std::atomic<uint64_t> table_counter;
    FILE* log;
private:
    rc_data():table_counter(0){}
    rc_data(const rc_data&) = delete;
    rc_data(rc_data&&) = delete;
    rc_data& operator= (const rc_data&) = delete;
    rc_data& operator= (rc_data&&) = delete;
public:
    void start(const char* location, alloc_ramcloud_buffer_t alloc, dealloc_ramcloud_buffer_t dealloc) {
        log = fopen("bt.log", "w");
        alloc_ = alloc;
        dealloc_ = dealloc;
    }
};

typedef util::singleton<rc_data, util::create_using_new<rc_data>, util::phoenix_lifetime<rc_data> > data_t;

void init_ram_cloud(const char* location, alloc_ramcloud_buffer_t alloc, dealloc_ramcloud_buffer_t dealloc)
{
    data_t::instance().start(location, alloc, dealloc);
}

void stop_ram_cloud() {
    auto & data = data_t::instance()._data;
    data_t::instance()._name_map.for_each([&data](std::string name, uint64_t table_id) {
        std::cout << name.c_str() << " has table id " << table_id;
        auto res = data.at(table_id);
        assert(res.first);
        std::cout << " and " << res.second->size() << " entries" << std::endl;
    });
}

void init_reject_rules(ramcloud_reject_rules_t *rules)
{
    rules->doesntExist = 0;
    rules->exists = 0;
    rules->givenVersion = 0;
    rules->versionLeGiven = 0;
    rules->versionNeGiven = 0;
}

ramcloud_error_t rc_create_table(const char* name)
{
    return rc_create_table_with_span(name, 1);
}

ramcloud_error_t rc_create_table_with_span(const char* name, uint32_t serverSpan)
{
    rc_data & data = data_t::instance();
    auto& m = data._name_map;
    std::string name_str(name);
    if (m.at(name_str).first)
            return STATUS_OK;

    auto create_table = [&name, &data](uint64_t & tid){
        if (tid == 0){
            //insert new id
            tid = ++data.table_counter;
            data._data.exec_on(tid, [](std::shared_ptr<rc_data::SingleTable>& table){
                if (table) {//table already exists
                    return false;
                }

                table = std::make_shared<rc_data::SingleTable>();
                return false;//keep new value
            });
        }
        return false;//keep this value
    };

    m.exec_on(name_str, create_table);
    return STATUS_OK;
}

ramcloud_error_t rc_drop_table(const char* name)
{
    rc_data & data = data_t::instance();
    bool erased;
    auto fun = [&data, &erased](uint64_t tableId){
            erased = data._data.erase(tableId).first;
            return true;//remove also this resolve entry
    };

    data._name_map.exec_on(name, fun);
    if (erased)
        return STATUS_OK;
    else
        return STATUS_TABLE_DOESNT_EXIST;
}

static ramcloud_error_t rc_get_table_id_impl(uint64_t* res, const char* name)
{
    auto& m = data_t::instance()._name_map;
    auto p = m.at(std::string(name));
    if (p.first) {
        *res = p.second;
        return STATUS_OK;
    }
    return STATUS_TABLE_DOESNT_EXIST;
}

ramcloud_error_t rc_get_table_id(uint64_t* res, const char* name)
{
    auto err = rc_get_table_id_impl(res, name);
    return err;
}


ramcloud_error_t rc_read(uint64_t tableId, const char* key,
                         uint16_t keyLength, ramcloud_buffer_t* value)
{
    auto l = current_thread_wait_for_turn();
    auto table_res = data_t::instance()._data.at(tableId);
    if (!table_res.first)
        return STATUS_TABLE_DOESNT_EXIST;
    std::shared_ptr<rc_data::SingleTable> table = table_res.second;
    ramcloud_error_t ret = STATUS_OK;
    auto fun = [&](decltype(table)::element_type::mapped_type & data){
        if (data.version == 0){
            ret = STATUS_OBJECT_DOESNT_EXIST;
            return true;
        }
        value->reset();
        value->data = data_t::instance().alloc_(data.size());
        value->length = uint32_t(data.size());
        memcpy(value->data, data.data(), value->length);
        return false;//keep record
    };
    table->exec_on(std::string(key,keyLength), fun);
    return ret;
}

ramcloud_error_t check_reject_rules(rc_data::Entry & entry, const ramcloud_reject_rules_t* rejectRules){
    if (!rejectRules)
        return STATUS_OK;
    if (entry.version){
        if (rejectRules->exists)
            return STATUS_OBJECT_EXISTS;
    }
    else {
        if (rejectRules->doesntExist)
            return STATUS_OBJECT_DOESNT_EXIST;
    }
    if (rejectRules->versionLeGiven) {
        if (!entry.version)
            return STATUS_OBJECT_DOESNT_EXIST;
        if (entry.version <= rejectRules->givenVersion){
            return STATUS_WRONG_VERSION;
        }
    }
    if (rejectRules->versionNeGiven) {
        if (entry.version != rejectRules->givenVersion){
            return STATUS_WRONG_VERSION;
        }
    }
    return STATUS_OK;
}

ramcloud_error_t rc_read_with_reject_impl(uint64_t tableId, const char* key,
                                          uint16_t keyLength, ramcloud_buffer_t* value,
                                          const ramcloud_reject_rules_t* rejectRules,
                                          uint64_t* version)
{
    auto l = current_thread_wait_for_turn();
    auto table_res = data_t::instance()._data.at(tableId);
    if (!table_res.first)
        return STATUS_TABLE_DOESNT_EXIST;
    std::shared_ptr<rc_data::SingleTable> table = table_res.second;
    ramcloud_error_t ret = STATUS_OK;
    auto fun = [&](decltype(table)::element_type::mapped_type & entry){
        if (entry.version == 0){
            ret = STATUS_OBJECT_DOESNT_EXIST;
            return true;//remove temporary record
        }
        if (version)
            *version = entry.version;
        ret = check_reject_rules(entry, rejectRules);
        if (ret)
            return false;
        value->data = data_t::instance().alloc_(entry.size());
        value->length = uint32_t(entry.size());
        memcpy(value->data, entry.data(), value->length);
        return false;//keep record
    };
    table->exec_on(std::string(key,keyLength), fun);
    return ret;
}

ramcloud_error_t rc_read_versioned(uint64_t tableId, const char* key,
                                          uint16_t keyLength, ramcloud_buffer_t* value,
                                          uint64_t* version)
{
    return rc_read_with_reject_impl(tableId, key, keyLength, value, nullptr, version);
}

ramcloud_error_t rc_read_with_reject(uint64_t tableId, const char* key,
                                     uint16_t keyLength, ramcloud_buffer_t* value,
                                     const ramcloud_reject_rules_t* rejectRules,
                                     uint64_t* version)
{
    return rc_read_with_reject_impl(tableId, key, keyLength, value, rejectRules, version);
}

ramcloud_error_t rc_increment(uint64_t tableId, const char* key,
                              uint16_t keyLength, int64_t incrementValue)
{
    return rc_increment_with_newreject(tableId, key, keyLength,
                                       incrementValue, nullptr, nullptr, nullptr);
}

ramcloud_error_t rc_increment_with_new(uint64_t tableId, const char* key,
                                       uint16_t keyLength, int64_t incrementValue,
                                       int64_t* newValue)
{
    return rc_increment_with_newreject(tableId, key, keyLength, incrementValue,
                                       nullptr, nullptr, newValue);
}

ramcloud_error_t rc_increment_with_reject(uint64_t tableId, const char* key,
                                          uint16_t keyLength, int64_t incrementValue,
                                          const ramcloud_reject_rules_t* rejectRules,
                                          uint64_t* version)
{
    return rc_increment_with_newreject(tableId, key, keyLength, incrementValue,
                                       rejectRules, version, nullptr);
}

ramcloud_error_t rc_increment_with_newreject(uint64_t tableId, const char* key,
                                             uint16_t keyLength, int64_t incrementValue,
                                             const ramcloud_reject_rules_t* rejectRules,
                                             uint64_t* version, int64_t* newValue)
{
    auto l = current_thread_wait_for_turn();
    auto table_res = data_t::instance()._data.at(tableId);
    if (!table_res.first)
        return STATUS_TABLE_DOESNT_EXIST;
    std::shared_ptr<rc_data::SingleTable> table = table_res.second;
    ramcloud_error_t ret = STATUS_OK;
    auto fun = [&](decltype(table)::element_type::mapped_type & entry){
        ret = check_reject_rules(entry, rejectRules);
        if (ret)
            return false;
        if (entry.version == 0) {
            ret = STATUS_OBJECT_DOESNT_EXIST;
            return true;
        }

        assert(entry.size() == sizeof(int64_t));
        int64_t* pcounter = reinterpret_cast<int64_t *>(entry._data.data());
        *pcounter += incrementValue;
        *newValue = *pcounter;
        ++entry.version;
        if (version)
            *version = entry.version;
        return false;//keep record
    };
    table->exec_on(std::string(key,keyLength), fun);
    return ret;
}

ramcloud_error_t rc_remove(uint64_t tableId, const char* key, uint16_t keyLength)
{
    return rc_remove_with_reject(tableId, key, keyLength, nullptr, nullptr);
}

ramcloud_error_t rc_remove_with_reject(uint64_t tableId,
                                       const char* key,
                                       uint16_t keyLength,
                                       const ramcloud_reject_rules_t* rejectRules,
                                       uint64_t* version)
{
    print_stacktrace(data_t::instance().log, 5, *reinterpret_cast<const uint64_t*>(key));
    auto l = current_thread_wait_for_turn();
    auto table_res = data_t::instance()._data.at(tableId);
    if (!table_res.first)
        return STATUS_TABLE_DOESNT_EXIST;
    std::shared_ptr<rc_data::SingleTable> table = table_res.second;
    ramcloud_error_t ret = STATUS_OK;
    auto fun = [&](decltype(table)::element_type::mapped_type & entry){
        if (entry.version == 0){
            ret = STATUS_OBJECT_DOESNT_EXIST;
            return true;//remove temporary record
        }
        if (version)
            *version = entry.version;
        ret = check_reject_rules(entry, rejectRules);
        if (ret)
            return false;
        return true;//remove record
    };
    table->exec_on(std::string(key,keyLength), fun);
    return ret;
}

static ramcloud_error_t rc_write_impl(uint64_t tableId, const char* key,
                                     uint16_t keyLength, const void* buf,
                                     uint32_t length,
                                     const ramcloud_reject_rules_t* rejectRules,
                                     uint64_t* version, bool async)
{
    auto l = current_thread_wait_for_turn();
    auto table_res = data_t::instance()._data.at(tableId);
    if (!table_res.first)
        return STATUS_TABLE_DOESNT_EXIST;
    std::shared_ptr<rc_data::SingleTable> table = table_res.second;
    ramcloud_error_t ret = STATUS_OK;
    auto fun = [&](decltype(table)::element_type::mapped_type & entry){
        ret = check_reject_rules(entry, rejectRules);
        if (ret) {
            if (ret == STATUS_OBJECT_DOESNT_EXIST)
                return true;//remove temporary record
            if (version)
                *version = entry.version;
            return false;
        }
        entry._data.resize(length);
        memcpy(entry._data.data(), buf, length);
        ++entry.version;
        if (version)
            *version = entry.version;
        return false;//keep record
    };
    table->exec_on(std::string(key,keyLength), fun);
    return ret;
}


extern "C" ramcloud_error_t rc_write_all(uint64_t tableId, const char* key,
                                     uint16_t keyLength, const void* buf,
                                     uint32_t length,
                                     const ramcloud_reject_rules_t* rejectRules,
                                     uint64_t* version, bool async)
{
    return rc_write_impl(tableId, key, keyLength, buf, length, rejectRules, version, async);
}

ramcloud_error_t rc_write(uint64_t tableId, const char* key,
                          uint16_t keyLength,
                          const char* s,
                          uint32_t len)
{
    return rc_write_all(tableId, key, keyLength, s, len, nullptr, nullptr, false);
}

ramcloud_error_t rc_write_async(uint64_t tableId,
                                const char* key,
                                uint16_t keyLength,
                                const char* s, uint32_t len)
{
    return rc_write_all(tableId, key, keyLength, s, len, nullptr, nullptr, true);
}

ramcloud_error_t rc_write_with_reject(uint64_t tableId, const char* key,
                                      uint16_t keyLength, const void* buf,
                                      uint32_t length,
                                      const ramcloud_reject_rules_t* rejectRules,
                                      uint64_t* version)
{
    return rc_write_all(tableId, key, keyLength, buf, length, rejectRules, version, false);
}

ramcloud_error_t rc_write_with_reject_async(uint64_t tableId, const char* key,
                                            uint16_t keyLength, const void* buf,
                                            uint32_t length,
                                            const ramcloud_reject_rules_t* rejectRules,
                                            uint64_t* version)
{
    return rc_write_all(tableId, key, keyLength, buf, length, rejectRules, version, true);
}








