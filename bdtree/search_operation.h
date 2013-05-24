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

template<typename Key, typename Value>
node_pointer<Key, Value>* lower_bound_node_with_context(const Key & key, operation_context<Key, Value>& context, search_bound bound, cache_use use_cache = cache_use::Current) {
    assert(!context.node_stack.empty());
    auto get_child = [&context, use_cache] (logical_pointer lptr) -> node_pointer<Key, Value>* {
        assert(lptr == context.node_stack.top());
        node_pointer<Key, Value>* np = context.cache.get_from_cache(lptr, context);
        if (np == nullptr)
            return nullptr;
        auto node_type = np->node_->get_node_type();
        if (node_type == node_type_t::LeafNode){
            np = use_cache == cache_use::Current ?
                        context.cache.get_current_from_cache(lptr, context)
                      : context.cache.get_without_cache(lptr, context);
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

template<typename Key, typename Value>
bdtree_iterator<Key, Value> lower_bound_with_context(const Key & key, operation_context<Key, Value>& context, search_bound bound) {
    return bdtree_iterator<Key, Value>(std::move(context), lower_bound_node_with_context(key, context, bound), key);
}

template<typename Key, typename Value>
std::pair<node_pointer<Key, Value>*, operation_context<Key, Value>> lower_node_bound(const Key & key, logical_table_cache<Key, Value>& cache, uint64_t tx_id) {
    operation_context<Key, Value> context{cache, tx_id};

    logical_pointer lptr{1};
    context.node_stack.push(lptr);
    node_pointer<Key, Value>* res = lower_bound_node_with_context(key, context, search_bound::LAST_SMALLER_EQUAL);
    return std::make_pair(res, std::move(context));
}

template<typename Key, typename Value>
bdtree_iterator<Key, Value> lower_bound(const Key & key, logical_table_cache<Key, Value>& cache, uint64_t tx_id) {
    auto res = lower_node_bound(key, cache, tx_id);
    return bdtree_iterator<Key, Value>(std::move(res.second), res.first, key);
}

}