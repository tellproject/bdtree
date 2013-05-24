#pragma once
#include <vector>

#include <serializer.h>
#ifndef NDEBUG
#include <unordered_set>
#endif

#include "forward_declarations.h"
#include "primitive_types.h"
#include "stacktrace.h"
#include "stl_specializations.h"

namespace bdtree {

enum class erase_result {
    Success,
    Failed,
    Merged
};

template<typename Key, typename Value>
struct operation_context {
    operation_context(logical_table_cache<Key, Value>& cache, uint64_t tx_id)
        :cache(cache), tx_id(tx_id) {}
    logical_table_cache<Key, Value>& cache;
    uint64_t tx_id;
    std::stack<logical_pointer> node_stack;
#ifndef NDEBUG
    std::unordered_set<logical_pointer> locks;
#endif
    logical_pointer_table get_ptr_table() const {
        return cache.get_ptr_table();
    }
    node_table get_node_table() const {
        return cache.get_node_table();
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
    }
    void* operator new(std::size_t size) {
        return awesome::malloc(size);
    }
    void* operator new[](std::size_t size) {
        return awesome::malloc(size);
    }
    void operator delete(void* ptr) {
        awesome::free_now(ptr);
    }
    void operator delete[](void* ptr) {
        awesome::free_now(ptr);
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
