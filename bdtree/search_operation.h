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

#include <algorithm>

#include "forward_declarations.h"
#include "util.h"
#include "iterator.h"

namespace bdtree {

enum class cache_use {
    None,
    Current
};

template<typename Key, typename Value, typename Backend>
node_pointer<Key, Value>* lower_bound_node_with_context(const Key & key,
        operation_context<Key, Value, Backend>& context, search_bound bound, cache_use use_cache = cache_use::Current) {
    assert(!context.node_stack.empty());
    auto get_child = [&context, use_cache] (logical_pointer lptr) -> node_pointer<Key, Value>* {
        assert(lptr == context.node_stack.top());
        node_pointer<Key, Value>* np = context.get_from_cache(lptr);
        if (np == nullptr)
            return nullptr;
        auto node_type = np->node_->get_node_type();
        if (node_type == node_type_t::LeafNode){
            np = use_cache == cache_use::Current ?
                        context.get_current_from_cache(lptr)
                      : context.get_without_cache(lptr);
        }
        return np;
    };
    auto lptr = context.node_stack.top();
    node_pointer<Key, Value>* np = get_child(lptr);
    if (np == nullptr) {
        np = fix_stack(key, context, bound);
    }
    key_compare<Key, Value> cmp;
    for (;;) {
        auto node_type = np->node_->get_node_type();
        if (node_type == node_type_t::InnerNode) {
            auto & n = *static_cast<inner_node<Key,Value>*>(np->node_);
            if (is_in_range(n, key, bound)) {
                auto iter = bound == search_bound::LAST_SMALLER_EQUAL ?
                            last_smaller_equal(n.array_.begin(), n.array_.end(), key, cmp)
                          : last_smaller(n.array_.begin(), n.array_.end(), key, cmp);
                lptr = iter->second;
                context.node_stack.push(lptr);
                np = get_child(lptr);
                if (np == nullptr) {
                    context.node_stack.pop();
                    np = fix_stack(key, context, bound);
                    continue;
                }
                continue;
            }
            else {
                np = fix_stack(key, context, bound);
                continue;
            }
        }
        else if (node_type == node_type_t::LeafNode){
            auto& leafnode = *static_cast<leaf_node<Key,Value>*>(np->node_);
            if (is_in_range(leafnode, key, bound)) {
                return np;
            }
            else {
                np = fix_stack(key, context, bound);
                continue;
            }
        }
        else {
            assert(false);
        }
        assert(false);
    }

}

template<typename Key, typename Value, typename Backend>
bdtree_iterator<Key, Value, Backend> lower_bound_with_context(const Key & key,
        operation_context<Key, Value, Backend>& context, search_bound bound) {
    return bdtree_iterator<Key, Value, Backend>(std::move(context), lower_bound_node_with_context(key, context, bound),
            key, bound);
}

template<typename Key, typename Value, typename Backend>
std::pair<node_pointer<Key, Value>*, operation_context<Key, Value, Backend>> lower_node_bound(const Key & key,
        Backend& backend, logical_table_cache<Key, Value, Backend>& cache, uint64_t tx_id) {
    operation_context<Key, Value, Backend> context{backend, cache, tx_id};

    logical_pointer lptr{1};
    context.node_stack.push(lptr);
    node_pointer<Key, Value>* res = lower_bound_node_with_context(key, context, search_bound::LAST_SMALLER_EQUAL);
    return std::make_pair(res, std::move(context));
}

template<typename Key, typename Value, typename Backend>
bdtree_iterator<Key, Value, Backend> lower_bound(const Key & key, Backend& backend,
        logical_table_cache<Key, Value, Backend>& cache, uint64_t tx_id) {
    auto res = lower_node_bound(key, backend, cache, tx_id);
    return bdtree_iterator<Key, Value, Backend>(std::move(res.second), res.first, key);
}

}
