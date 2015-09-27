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
#include <bdtree/stl_specializations.h>
#include <bdtree/serializer.h>

#include <crossbow/allocator.hpp>

#include <stack>
#include <vector>

#ifndef NDEBUG
#include <unordered_set>
#endif

namespace bdtree {

enum class erase_result {
    Success,
    Failed,
    Merged
};

template<typename Key, typename Value, typename Backend>
struct operation_context {
    operation_context(Backend& backend, logical_table_cache<Key, Value, Backend>& cache, uint64_t tx_id)
        : backend(backend), cache(cache), tx_id(tx_id) {}
    Backend& backend;
    logical_table_cache<Key, Value, Backend>& cache;
    uint64_t tx_id;
    std::stack<logical_pointer> node_stack;
#ifndef NDEBUG
    std::unordered_set<logical_pointer> locks;
#endif
    typename Backend::ptr_table& get_ptr_table() {
        return backend.get_ptr_table();
    }
    typename Backend::node_table& get_node_table() const {
        return backend.get_node_table();
    }

    node_pointer<Key, Value>* get_from_cache(logical_pointer lptr) {
        return cache.get_from_cache(lptr, *this);
    }

    node_pointer<Key, Value>* get_current_from_cache(logical_pointer lptr) {
        return cache.get_current_from_cache(lptr, *this);
    }

    node_pointer<Key, Value>* get_without_cache(logical_pointer lptr) {
        return cache.get_without_cache(lptr, *this);
    }
};

template<typename Key, typename Value>
struct operation {
    virtual bool visit(node_pointer<Key, Value>& node) = 0;
    virtual bool visit(inner_node<Key, Value>& node) = 0;
    virtual bool visit(leaf_node<Key, Value>& node) = 0;
    virtual bool visit(insert_delta<Key, Value>& node) = 0;
    virtual bool visit(delete_delta<Key, Value>& node) = 0;
    virtual bool visit(split_delta<Key, Value>& node) = 0;
    virtual bool visit(remove_delta<Key, Value>& node) = 0;
    virtual bool visit(merge_delta<Key, Value>& node) = 0;
};

template<typename Key, typename Value>
struct node {
public:
    typedef Key key_type;
public:
    virtual ~node() {}
public: // operations
    virtual bool accept(operation<Key, Value>&) = 0;
public:
    virtual std::vector<uint8_t> serialize() const = 0;
    virtual node_type_t get_node_type() const = 0;
public: // new/delete
    virtual node<Key, Value>* copy() const {
        assert(false);
        return nullptr;
    }
    void* operator new(std::size_t size) {
        return crossbow::allocator::malloc(size);
    }
    void* operator new[](std::size_t size) {
        return crossbow::allocator::malloc(size);
    }
    void operator delete(void* ptr) {
        crossbow::allocator::free_now(ptr);
    }
    void operator delete[](void* ptr) {
        crossbow::allocator::free_now(ptr);
    }
};

template<template<typename, typename> class T, typename Key, typename Value>
struct serializable_node : T<Key, Value> {

    typedef T<Key, Value> parent;

    template<typename... Args>
    serializable_node(Args&&... args) : T<Key, Value>(std::forward<Args>(args)...) {}

    template<typename Archiver>
    void visit(Archiver& ar) const {
        const_cast<serializable_node<T, Key, Value>*>(this)->visit(ar);
    }

    node_type_t get_node_type() const override {
        return T<Key, Value>::node_type;
    }

    node<Key, Value>* copy() const override {
        return new serializable_node<T, Key, Value>(*this);
    }
    
    using T<Key, Value>::visit;

    std::vector<uint8_t> serialize() const override {
        std::size_t size = serialized_size();
        std::vector<uint8_t> res(size);
        res[0] = uint8_t(T<Key, Value>::node_type);
        static_assert(sizeof(uint8_t) == sizeof(parent::node_type), "Can not assign node type to uint8_t");
        serializer_into_array ser(res.data() + sizeof(parent::node_type));
        ser & *this;
        return res;
    }

    std::size_t serialized_size() const {
        sizer s;
        s & *this;
        return s.size + sizeof(parent::node_type);
    }

    bool accept(operation<Key, Value>& o) override {
        return o.visit(*this);
    }
};

// TODO: Implement
template<typename Key, typename Value>
node<Key, Value>* deserialize(const uint8_t* ptr, uint64_t size, physical_pointer pptr) {
    node_type_t type = node_type_t(*ptr);
    switch (type) {
    case node_type_t::InnerNode:
    {
        inner_node<Key, Value> *res = new inner_node<Key, Value>(pptr);
        ::deserialize(*res, ptr + 1);
        return res;
    }
    case node_type_t::LeafNode:
    {
        leaf_node<Key, Value> *res = new leaf_node<Key, Value>(pptr);
        ::deserialize(*res, ptr + 1);
        return res;
    }
    case node_type_t::InsertDelta:
    {
        insert_delta<Key, Value> *res = new insert_delta<Key, Value>();
        ::deserialize(*res, ptr + 1);
        return res;
    }
    case node_type_t::DeleteDelta:
    {
        delete_delta<Key, Value> *res = new delete_delta<Key, Value>();
        ::deserialize(*res, ptr + 1);
        return res;
    }
    case node_type_t::SplitDelta:
    {
        split_delta<Key, Value> *res = new split_delta<Key, Value>();
        ::deserialize(*res, ptr + 1);
        return res;
    }
    case node_type_t::RemoveDelta:
    {
        remove_delta<Key, Value> *res = new remove_delta<Key, Value>();
        ::deserialize(*res, ptr + 1);
        return res;
    }
    case node_type_t::MergeDelta:
    {
        merge_delta<Key, Value> *res = new merge_delta<Key, Value>();
        ::deserialize(*res, ptr + 1);
        return res;
    }
    default:
        assert(false);
        break;
    }
    return nullptr;
}
}
