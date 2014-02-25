#pragma once

#include <functional>
#include <limits>
#include <cassert>

#include "base_types.h"
#include "forward_declarations.h"
#include "logical_table_cache.h"
#include "primitive_types.h"


namespace bdtree {

template<typename NodeType>
struct node_delete;

template<typename Key, typename Value>
struct node_delete<inner_node<Key, Value> > {
    static void rem(inner_node<Key, Value>& inner, physical_pointer pptr, operation_context<Key, Value>& context) {
        __attribute__((unused)) auto rc_res = rc_remove(context.get_node_table().value, pptr.value_ptr(), pptr.length);
        assert(rc_res == STATUS_OK);
    }
};

template<typename Key, typename Value>
struct node_delete<leaf_node<Key, Value> > {
    static void rem(leaf_node<Key, Value>& leaf, physical_pointer pptr, operation_context<Key, Value>& context) {
        __attribute__((unused)) auto rc_res = rc_remove(context.get_node_table().value, leaf.leaf_pptr_.value_ptr(), leaf.leaf_pptr_.length);
        assert(rc_res == STATUS_OK);
        for (physical_pointer ptr : leaf.deltas_) {
            assert(ptr != leaf.leaf_pptr_);
            rc_res = rc_remove(context.get_node_table().value, ptr.value_ptr(), ptr.length);
            assert(rc_res == STATUS_OK);
        }
    }
};

template<typename Key>
struct null_key {
    static_assert(std::numeric_limits<Key>::is_specialized, "null_key not defined");
    static constexpr Key value() {
        return std::numeric_limits<Key>::min();
    }
};

template<>
struct null_key<std::string> {
    static constexpr const char* value() {
        return "";
    }
};

template<typename T, typename U>
struct null_key<std::pair<T,U> > {
    static constexpr std::pair<T, U> value() {
        return std::make_pair(null_key<T>::value(),null_key<U>::value());
    }
};

template<typename T, typename... U>
struct null_key<std::tuple<T,U...> > {
    static constexpr std::tuple<T, U...> value() {
        return std::tuple_cat(std::forward_as_tuple(null_key<T>::value()),null_key<std::tuple<U...> >::value());
    }
};

template<typename T>
struct null_key<std::tuple<T> > {
    static constexpr std::tuple<T> value() {
        return std::forward_as_tuple(null_key<T>::value());
    }
};

template<typename ForwardIt, typename T, typename Compare>
ForwardIt last_smaller_equal(ForwardIt first, ForwardIt last, const T& value, Compare cmp) {
    return --std::upper_bound(first, last, value, cmp);
}

template<typename ForwardIt, typename T, typename Compare>
ForwardIt last_smaller(ForwardIt first, ForwardIt last, const T& value, Compare cmp) {
    return --std::lower_bound(first, last, value, cmp);
}

template<typename Node, typename Key>
bool is_in_range(Node& n, const Key& key, search_bound bound) {
    if (bound == search_bound::LAST_SMALLER) {
        bool high_key_ok = !n.high_key_ || key < n.high_key_ || key == n.high_key_;
        if (n.low_key_ == null_key<Key>::value())
            return high_key_ok;
        if (n.low_key_ < key)
            return high_key_ok;
    } else {
        if (key < n.low_key_)
            return false;
        if (!n.high_key_)//infinity
            return true;
        if (key < n.high_key_)
            return true;
    }
    return false;
}

struct guard {
    std::function<bool ()> asserts;
    ~guard() {
        assert(asserts());
    }
};

//re-reads the nodes in context.node_stack from bottom to top without cache
template<typename Key, typename Value>
node_pointer<Key, Value>* fix_stack(const Key& key, operation_context<Key, Value>& context, search_bound bound) {
    assert(!context.node_stack.empty());
    node_pointer<Key, Value>* np = nullptr;
#ifndef NDEBUG
    __attribute__ ((unused)) guard _{[&np]() {
        return np && np->node_;
    }};
#endif
    for (;;) {
        auto lptr = context.node_stack.top();
        np = context.cache.get_without_cache(lptr, context);
        if (np == nullptr) {
            context.node_stack.pop();
            assert(!context.node_stack.empty());
            continue;
        }
        auto node_type = np->node_->get_node_type();
        if (node_type == node_type_t::InnerNode) {
            if (is_in_range(*static_cast<inner_node<Key,Value>*>(np->node_), key, bound))
                return np;
        }
        else if (node_type == node_type_t::LeafNode){
            if (is_in_range(*static_cast<leaf_node<Key,Value>*>(np->node_), key, bound))
                return np;
        }
        else {
            assert(false);
        }
        context.node_stack.pop();
        assert(!context.node_stack.empty());
    }
}

template<typename Key, typename Value>
node_pointer<Key, Value>* get_next(operation_context<Key, Value>& context, node_pointer<Key, Value>* current) {
    assert(current);
    if (!current->as_leaf()->high_key_) {//at the end
        assert(false);
        return nullptr;
    }
    auto current_lptr = current->lptr_;
    for (;;) {
        //1. try reading next
        auto np = context.cache.get_current_from_cache(current->as_leaf()->right_link_, context);
        if (np) {
            assert(np->lptr_ == current->as_leaf()->right_link_);
            if (np->node_->get_node_type() == node_type_t::LeafNode) {
                assert(!current->as_leaf()->high_key_ || np->as_leaf()->low_key_ == *current->as_leaf()->high_key_);
            }
            return np;
        }
        //2. reread current, and retry reading next
        np = context.cache.get_without_cache(current_lptr, context);
        if (np == nullptr) {//current is deleted
            break;
        }
        auto* lnode = np->as_leaf();
        if (!lnode->high_key_) {
            return np;
        }
        if (is_in_range(*lnode, *current->as_leaf()->high_key_, search_bound::LAST_SMALLER_EQUAL)){
            return np;
        }
        current = np;
    }
    //3. search for n.high_key_
    if (context.node_stack.size() > 1)
        context.node_stack.pop();
    auto iter = lower_bound_with_context<Key,Value>(*current->as_leaf()->high_key_, context);
    return iter.current_;
}

template<typename Key, typename Value>
node_pointer<Key, Value>* get_previous(operation_context<Key, Value>& context, node_pointer<Key, Value>* current) {
    assert(current);
    if (current->as_leaf()->low_key_ == null_key<Key>::value()) {
        assert(false);
        return nullptr;
    }
    context.node_stack.pop();
    auto iter = lower_bound_with_context<Key,Value>(current->as_leaf()->low_key_, context, search_bound::LAST_SMALLER);
    return iter.current_;
}

}
