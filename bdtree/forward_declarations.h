#pragma once

namespace bdtree {

template<typename Key, typename Value>
struct node;

template<typename Key, typename Value>
struct logical_table_cache;

template<template<typename, typename> class T, typename Key, typename Value>
struct serializable_node;
template<typename Key, typename Value>
struct node_pointer;

template<typename Key, typename Value>
struct inner_node_t;
template<typename Key, typename Value>
using inner_node = serializable_node<inner_node_t, Key, Value>;

template<typename Key, typename Value>
struct leaf_node_t;
template<typename Key, typename Value>
using leaf_node = serializable_node<leaf_node_t, Key, Value>;

template<typename Key, typename Value>
struct insert_delta_t;
template<typename Key, typename Value>
using insert_delta = serializable_node<insert_delta_t, Key, Value>;

template<typename Key, typename Value>
struct delete_delta_t;
template<typename Key, typename Value>
using delete_delta = serializable_node<delete_delta_t, Key, Value>;

template<typename Key, typename Value>
struct split_delta_t;
template<typename Key, typename Value>
using split_delta = serializable_node<split_delta_t, Key, Value>;

template<typename Key, typename Value>
struct remove_delta_t;
template<typename Key, typename Value>
using remove_delta = serializable_node<remove_delta_t, Key, Value>;

template<typename Key, typename Value>
struct merge_delta_t;
template<typename Key, typename Value>
using merge_delta = serializable_node<merge_delta_t, Key, Value>;


template<typename Key, typename Value>
struct operation;
template<typename Key, typename Value>
struct resolve_operation;
template<typename Key, typename Value>
struct split_operation;
template<typename Key, typename Value>
struct merge_operation;

enum class search_bound {
    LAST_SMALLER,
    LAST_SMALLER_EQUAL
};

struct empty_t;

template<typename Key, typename Value>
struct bdtree_iterator;
template<typename Key, typename Value>
struct operation_context;
template<typename ForwardIt, typename T, typename Compare>
ForwardIt last_smaller_equal(ForwardIt first, ForwardIt last, const T& value, Compare cmp);
template<typename ForwardIt, typename T, typename Compare>
ForwardIt last_smaller(ForwardIt first, ForwardIt last, const T& value, Compare cmp);
template<typename Key, typename Value>
bdtree_iterator<Key, Value> lower_bound_with_context(const Key & key, operation_context<Key, Value>& context, search_bound bound = search_bound::LAST_SMALLER_EQUAL);
}
