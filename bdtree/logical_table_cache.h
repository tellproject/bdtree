/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#pragma once

#include <bdtree/forward_declarations.h>
#include <bdtree/primitive_types.h>
#include <bdtree/base_types.h>
#include <bdtree/acache.h>
#include <bdtree/error_code.h>

#include <crossbow/allocator.hpp>

#include <algorithm>
#include <iostream>

namespace bdtree {
    template<typename Key, typename Value, typename Backend>
    struct logical_table_cache {
    public:
        logical_table_cache() = default;
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
        node_pointer<Key, Value>* get_from_cache(logical_pointer lptr,
                operation_context<Key, Value, Backend>& context) {
            auto tx_id = context.tx_id;
            context.tx_id = 0;
            auto ret = get_current_from_cache(lptr, context);
            context.tx_id = tx_id;
            return ret;
        }

        node_pointer<Key, Value>* get_current_from_cache(logical_pointer lptr,
                operation_context<Key, Value, Backend>& context) {
            assert(lptr.value != 0);
            auto res = map_.at(lptr);
            if (res.first && res.second->last_tx_id_.load() >= context.tx_id && res.second->resolve(context))
                return res.second;
            return get_without_cache(lptr, context);
        }

        node_pointer<Key, Value>* get_without_cache(logical_pointer lptr,
                    operation_context<Key,Value, Backend>& context) {
            bool resolve_succ = false;
            node_pointer<Key, Value>* result;
            while (!resolve_succ) {
                auto txid = get_last_tx_id();
                auto& ptr_table = context.get_ptr_table();
                std::error_code ec;
                auto pptr = ptr_table.read(lptr, ec);
                if (ec == error::object_doesnt_exist)
                    return nullptr;
                assert(!ec);
                auto np = new node_pointer<Key, Value>(lptr, std::get<0>(pptr), std::get<1>(pptr));
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
                crossbow::allocator::destroy(do_delete);
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
            crossbow::allocator::destroy(do_delete);
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
            crossbow::allocator::destroy(do_delete);
        }

        void print_statistics(operation_context<Key, Value, Backend>& context) {
            uint64_t counter = 0;
            uint64_t max_chain = 0;
            uint64_t chain_sum = 0;
            uint64_t items = 0;
            map_.for_each([this, &counter, &max_chain, &items, &chain_sum, &context]
                    (logical_pointer lptr, node_pointer<Key, Value>* e) {
                ++items;
                uint64_t lptr_version;
                std::error_code ec;
                context.get_ptr_table().read(lptr, lptr_version, ec);
                if (ec == error::object_doesnt_exist) {
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

