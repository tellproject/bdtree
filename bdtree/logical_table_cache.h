#pragma once
#include <algorithm>
#include <iostream>

#include "forward_declarations.h"
#include "primitive_types.h"
#include "base_types.h"
#include <cramcloud.h>
#include "concurrent_map.h"
#include "acache.h"

namespace bdtree {
	template<typename Key, typename Value>
    struct logical_table_cache {
        logical_pointer_table lpt_;
        node_table nt_;
    public:
        explicit logical_table_cache(logical_pointer_table ltid, node_table ntid) : lpt_(ltid), nt_(ntid) {}
        ~logical_table_cache() {
            map_.for_each([](logical_pointer lptr, node_pointer<Key, Value>* e) {
                delete e;
            });
        }

        logical_table_cache(const logical_table_cache&) = delete;
        logical_table_cache(logical_table_cache&&) = delete;
        logical_table_cache& operator= (const logical_table_cache&) = delete;
        logical_table_cache& operator= (logical_table_cache&&) = delete;
    private:
        cache<Key, Value> map_;
    public:
        logical_pointer_table get_ptr_table() const { return lpt_; }
        node_table get_node_table() const { return nt_; }
        logical_pointer get_next_logical_ptr() {
            uint64_t zero = 0;
            uint64_t res;
            __attribute__((unused)) auto err = rc_increment_with_new(lpt_.value, (const char*)&zero, sizeof(zero), 1, reinterpret_cast<int64_t*>(&res));
            assert(err == STATUS_OK);
            return logical_pointer{res};
        }
        physical_pointer get_next_physical_ptr() {
            uint64_t zero = 0;
            uint64_t res;
            __attribute__((unused)) auto err = rc_increment_with_new(nt_.value, (const char*) &zero, sizeof(zero), 1, reinterpret_cast<int64_t*>(&res));
            assert(err == STATUS_OK);
            return physical_pointer{res};
        }
        node_pointer<Key, Value>* get_from_cache(logical_pointer lptr, operation_context<Key, Value>& context) {
            auto tx_id = context.tx_id;
            context.tx_id = 0;
            auto ret = get_current_from_cache(lptr, context);
            context.tx_id = tx_id;
            return ret;
        }

        node_pointer<Key, Value>* get_current_from_cache(logical_pointer lptr, operation_context<Key, Value>& context) {
            assert(lptr.value != 0);
            auto res = map_.at(lptr);
            if (res.first && res.second->last_tx_id_.load() >= context.tx_id && res.second->resolve(context))
                return res.second;
            return get_without_cache(lptr, context);
        }

        node_pointer<Key, Value>* get_without_cache(logical_pointer lptr, operation_context<Key,Value>& context) {
            bool resolve_succ = false;
            node_pointer<Key, Value>* result;
            while (!resolve_succ) {
                auto txid = get_last_tx_id();
                ramcloud_buffer buf;
                uint64_t rc_version;
                auto err = rc_read_versioned(lpt_.value, (const char*) &lptr.value, sizeof(lptr.value), &buf, &rc_version);
                if (err == STATUS_OBJECT_DOESNT_EXIST)
                    return nullptr;
                assert(err == STATUS_OK);
                auto np = new node_pointer<Key, Value>(lptr, physical_pointer{*reinterpret_cast<uint64_t*>(buf.data)}, rc_version);
                decltype(np) todel = nullptr;
                map_.exec_on(lptr, [&np, txid, &todel](node_pointer<Key, Value>*& e){
                    todel = nullptr;
                    np->release_old();
                    bool did_write = false;
                    if (e == nullptr || e->rc_version_ < np->rc_version_) {
                        np->reset_old(e);
                        e = np;
                        did_write = true;
                    } else {
                        todel = np;
                        np = e;
                    }
                    for (;;) {
                        auto lasttx = e->last_tx_id_.load();
                        auto nlasttx = std::max(txid, lasttx);
                        if (lasttx != nlasttx) {
                            if (e->last_tx_id_.compare_exchange_strong(lasttx, nlasttx)) {
                                return did_write ? cache_return::Write : cache_return::Read;
                            }
                        } else {
                            return did_write ? cache_return::Write : cache_return::Read;
                        }
                    }
                });
                if (todel) delete todel;
                if (np->resolve(context)) {
                    resolve_succ = true;
                }
                result = np;
            }
            return result;
        }

        //returns true if node was successfully added to the cache
        bool add_entry(node_pointer<Key, Value>* node, uint64_t txid) {
            bool result = true;
            node_pointer<Key, Value>* do_delete = nullptr;
            map_.exec_on(node->lptr_, [node, txid, &result, &do_delete](node_pointer<Key, Value>*& e) {
                do_delete = nullptr;
                if (!e) {
                    node->last_tx_id_ = txid;
                    e = node;
                    return cache_return::Write;
                } else if (e->rc_version_ < node->rc_version_) {
                    do_delete = e;
                    e = node;
                    for (;;) {
                        auto lasttx = e->last_tx_id_.load();
                        auto nlasttx = std::max(txid, lasttx);
                        if (lasttx != nlasttx) {
                            if (e->last_tx_id_.compare_exchange_strong(lasttx, nlasttx)) {
                                return cache_return::Write;
                            }
                        } else {
                            return cache_return::Write;
                        }
                    }
                } else {
                    result = false;
                    return cache_return::Nop;
                }
                assert(false);
                return cache_return::Nop;
            });
            if (do_delete)
                awesome::mark_for_deletion(do_delete);
            return result;
        }

        void invalidate(logical_pointer lptr) {
            node_pointer<Key, Value>* do_delete = nullptr;
            map_.exec_on(lptr, [&do_delete](node_pointer<Key, Value>*& e) {
                if (e) {
                    do_delete = e;
                    return cache_return::Remove;
                }
                else {
                    do_delete = nullptr;
                    return cache_return::Nop;
                }
            });
            awesome::mark_for_deletion(do_delete);
        }
        
        void invalidate_if_older(logical_pointer lptr, uint64_t rc_version) {
            node_pointer<Key, Value>* do_delete;
            map_.exec_on(lptr, [rc_version, &do_delete](node_pointer<Key, Value>*& e){
                do_delete = nullptr;
                if (e == nullptr)
                    return cache_return::Nop;
                if (e->rc_version_ < rc_version) {
                    do_delete = e;
                    return cache_return::Remove;
                }
                return cache_return::Nop;
            });
            awesome::mark_for_deletion(do_delete);
        }

        void print_statistics() {
            uint64_t counter = 0;
            uint64_t max_chain = 0;
            uint64_t chain_sum = 0;
            uint64_t items = 0;
            map_.for_each([this, &counter, &max_chain, &items, &chain_sum](logical_pointer lptr, node_pointer<Key, Value>* e){
                ++items;
                ramcloud_buffer buf;
                if (rc_read(this->get_ptr_table().value, lptr.value_ptr(), lptr.length, &buf) == STATUS_OBJECT_DOESNT_EXIST) {
                    ++counter;
                }
                uint64_t chain_length = 0;
                auto* ptr = e;
                while (ptr && ptr->old_) {
                    ++chain_length;
                    ++chain_sum;
                    ptr = ptr->old_.get();
                }
                max_chain = std::max(max_chain, chain_length);
            });
            std::cout << "outdated cache entries: " << counter << std::endl;
            std::cout << "max_chain length: " << max_chain << std::endl;
            std::cout << "avg chain length: " << double(chain_sum)/items << std::endl;
        }
    };
}

