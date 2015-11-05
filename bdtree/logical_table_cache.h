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

// TODO: reimplement cache
namespace bdtree {
    template<typename Key, typename Value, typename Backend>
    struct logical_table_cache {
    public:
        logical_table_cache() = default;
        logical_table_cache(const logical_table_cache&) = delete;
        logical_table_cache(logical_table_cache&&) = delete;
        logical_table_cache& operator= (const logical_table_cache&) = delete;
        logical_table_cache& operator= (logical_table_cache&&) = delete;
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
            return get_without_cache(lptr, context);
        }

        node_pointer<Key, Value>* get_without_cache(logical_pointer lptr,
                    operation_context<Key,Value, Backend>& context) {
            bool resolve_succ = false;
            node_pointer<Key, Value>* result;
            while (!resolve_succ) {
                auto& ptr_table = context.get_ptr_table();
                std::error_code ec;
                auto pptr = ptr_table.read(lptr, ec);
                if (ec == error::object_doesnt_exist)
                    return nullptr;
                assert(!ec);
                auto np = new node_pointer<Key, Value>(lptr, std::get<0>(pptr), std::get<1>(pptr));
                decltype(np) todel = nullptr;
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
            return result;
        }

        void invalidate(logical_pointer) {
        }
        
        void invalidate_if_older(logical_pointer lptr, uint64_t rc_version) {
        }

        void print_statistics(operation_context<Key, Value, Backend>& context) {
        }
    };
}

