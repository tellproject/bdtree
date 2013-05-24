#pragma once
#include "bdtree/primitive_types.h"
#include "bdtree/node_pointer.h"
#include "../amalloc.h"

#include <array>
#include <vector>
#include <functional>

namespace bdtree {

template<typename T>
struct double_word_atomic {
private:
    __int128 data;
public:
    double_word_atomic() {
        new (&data)T();
    }
    static_assert(sizeof(T) <= 16, "Type is too big");
    static_assert(sizeof(T) > 8, "One should not use double_word_atomic for types smaller than 8");

    T load() {
        __int128 res = __atomic_load_n(&data, __ATOMIC_SEQ_CST);
        return reinterpret_cast<T&>(res);
    }
    void store(T desired) {
        __atomic_store_n(&data, reinterpret_cast<__int128&>(desired), __ATOMIC_SEQ_CST);
    }

    bool cas(T& expected, T desired) {
        return __atomic_compare_exchange(&data, reinterpret_cast<__int128*>(&expected), reinterpret_cast<__int128*>(&desired), false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    }
};

enum class cache_return {
    Nop,
    Read,
    Write,
    Remove
};

template<typename Key, typename Value, size_t Size = 1024>
class cache {
    struct table_entry {
        logical_pointer lptr = logical_pointer{0};
        node_pointer<Key, Value>* ptr = nullptr;
        void reset() {
            lptr = logical_pointer{0};
            ptr = nullptr;
        }
    };
    struct entry_array {
        std::array<double_word_atomic<table_entry>, 3> entries;
        std::atomic<uint32_t> order;
        entry_array() {
            uint8_t* o = reinterpret_cast<uint8_t*>(&order);
            o[0] = 0;
            o[1] = 1;
            o[2] = 2;
        }
    };
    std::vector<entry_array> vec_;
    std::hash<uint64_t> hasher;
public:
    cache() : vec_(Size) {}

    // WARNING: this method is not thread safe
    template<typename Fun>
    void for_each(const Fun& fun) {
        for (size_t i = 0; i < Size; ++i) {
            for (uint8_t j = 0; j < 3; ++j) {
                table_entry e = vec_[i].entries[j].load();
                fun(e.lptr, e.ptr);
            }
        }
    }

    std::pair<bool, node_pointer<Key, Value>*> at(logical_pointer lptr) {
        auto index = hasher(lptr.value) % Size;
        entry_array& e = vec_[index];
        for (uint8_t i = 0; i < 3; ++i) {
            table_entry entry = e.entries[i].load();
            if (entry.lptr == lptr) {
                auto o = e.order.load();
                auto old_order = o;
                uint8_t* order = reinterpret_cast<uint8_t*>(&o);
                if (order[0] == i) {
                }else if (order[1] == i) {
                    order[1] = order[0];
                    order[0] = i;
                } else {
                    order[2] = order[1];
                    order[1] = order[0];
                    order[0] = i;
                }
                e.order.compare_exchange_strong(old_order, o);
                return std::make_pair(true, entry.ptr);
            }
        }
        return std::make_pair(false, nullptr);
    }

    template<typename Fun>
    void exec_on(logical_pointer lptr, const Fun& fun) {
        auto index = hasher(lptr.value) % Size;
        for (;;) {
            entry_array& e = vec_[index];
            table_entry entry;
            uint8_t i;
            bool found = false;
            for (i = 0; i < 3; ++i) {
                entry = e.entries[i].load();
                assert(entry.ptr == nullptr || entry.lptr.value != reinterpret_cast<uint64_t>(entry.ptr));
                if (entry.lptr == lptr) {
                    found = true;
                    break;
                }
            }
            table_entry old_en = entry;
            if (!found) {
                auto o = e.order.load();
                i = reinterpret_cast<uint8_t*>(&o)[2];
                old_en = e.entries[i].load();
                entry.reset();
                entry.lptr = lptr;
            }
            cache_return ret = fun(entry.ptr);
            switch (ret) {
            case cache_return::Nop:
                return;
            case cache_return::Read:
            {
                auto o = e.order.load();
                auto old_order = o;
                uint8_t* order = reinterpret_cast<uint8_t*>(&o);
                if (order[0] == i) {
                } else if (order[1] == i) {
                    order[1] = order[0];
                    order[0] = i;
                } else {
                    order[2] = order[1];
                    order[1] = order[0];
                    order[0] = i;
                }
                e.order.compare_exchange_strong(old_order, o);
                return;
            }
            case cache_return::Remove:
            {
                if (!found) return;
                // try to remove the element
                entry.reset();
                if (e.entries[i].cas(old_en, entry)) {
                    // if succeeded, try to reoder the elements
                    auto o = e.order.load();
                    auto old_order = o;
                    uint8_t *order = reinterpret_cast<uint8_t*>(&o);
                    if (order[0] == i) {
                        order[0] = order[1];
                        order[1] = order[2];
                        order[2] = i;
                    } else if (order[1] == i) {
                        order[1] = order[2];
                        order[2] = i;
                    } else {
                        return;
                    }
                    e.order.compare_exchange_strong(old_order, o);
                    return;
                }
                continue;
            }
            case cache_return::Write:
            {
                // write back value
                if (e.entries[i].cas(old_en, entry)) {
                    // if success, update access list
                    if (old_en.ptr && !found) {
                        awesome::mark_for_deletion(old_en.ptr);
                    }
                    auto o = e.order.load();
                    auto old_order = o;
                    uint8_t* order = reinterpret_cast<uint8_t*>(&o);
                    if (order[0] == i) {
                        return;
                    }else if (order[1] == i) {
                        order[1] = order[0];
                        order[0] = i;
                    } else {
                        order[2] = order[1];
                        order[1] = order[0];
                        order[0] = i;
                    }
                    e.order.compare_exchange_strong(old_order, o);
                    return;
                }
            }
            }
        }
    }
};

}
