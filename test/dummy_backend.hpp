#pragma once

#include <bdtree/base_backend.h>
#include <bdtree/error_code.h>
#include <bdtree/stl_specializations.h>

#include <crossbow/non_copyable.hpp>

#include <tbb/spin_rw_mutex.h>
#include <tbb/concurrent_unordered_map.h>

#include <atomic>
#include <cstdint>
#include <vector>

class dummy_node_data {
public:
    dummy_node_data(const std::vector<char>& data)
            : data_(data) {
    }

    const char* data() const {
        return data_.data();
    }

    size_t length() const {
        return data_.size();
    }

private:
    std::vector<char> data_;
};

class dummy_ptr_table : public bdtree::base_ptr_table<dummy_ptr_table> {
public:
    dummy_ptr_table()
            : counter_(0x0u) {
    }

    bdtree::logical_pointer get_next_ptr() {
        return {++counter_};
    }

    bdtree::logical_pointer get_remote_ptr() {
        return {counter_.load()};
    }

    std::tuple<bdtree::physical_pointer, uint64_t> read(bdtree::logical_pointer lptr, std::error_code& ec) {
        typename decltype(ptrs_mutex_)::scoped_lock _(ptrs_mutex_, false);
        auto i = ptrs_.find(lptr);
        if (i == ptrs_.end()) {
            ec = make_error_code(bdtree::error::object_doesnt_exist);
            return std::make_tuple(bdtree::physical_pointer(), 0x0u);
        }

        return i->second;
    }

    using bdtree::base_ptr_table<dummy_ptr_table>::read;

    uint64_t insert(bdtree::logical_pointer lptr, bdtree::physical_pointer pptr, std::error_code& ec) {
        typename decltype(ptrs_mutex_)::scoped_lock _(ptrs_mutex_, false);
        auto res = ptrs_.emplace(lptr, std::make_tuple(pptr, 0x1ull));
        if (!res.second) {
            ec = make_error_code(bdtree::error::object_exists);
        }
        return std::get<1>(res.first->second); // Get version from iterator
    }

    using bdtree::base_ptr_table<dummy_ptr_table>::insert;

    uint64_t update(bdtree::logical_pointer lptr, bdtree::physical_pointer pptr, uint64_t version,
            std::error_code& ec) {
        typename decltype(ptrs_mutex_)::scoped_lock _(ptrs_mutex_, true);
        auto i = ptrs_.find(lptr);
        if (i == ptrs_.end()) {
            ec = make_error_code(bdtree::error::object_doesnt_exist);
            return 0x0u;
        }
        auto v = std::get<1>(i->second);
        if (v > version) {
            ec = make_error_code(bdtree::error::wrong_version);
            return v;
        }
        i->second = std::make_tuple(pptr, v + 1);
        return v + 1;
    }

    using bdtree::base_ptr_table<dummy_ptr_table>::update;

    void remove(bdtree::logical_pointer lptr, uint64_t version, std::error_code& ec) {
        typename decltype(ptrs_mutex_)::scoped_lock _(ptrs_mutex_, true);
        auto i = ptrs_.find(lptr);
        if (i == ptrs_.end()) {
            ec = make_error_code(bdtree::error::object_doesnt_exist);
            return;
        }
        auto v = std::get<1>(i->second);
        if (v > version) {
            ec = make_error_code(bdtree::error::wrong_version);
            return;
        }
        ptrs_.unsafe_erase(i);
    }

    using bdtree::base_ptr_table<dummy_ptr_table>::remove;

private:
    std::atomic<uint64_t> counter_;

    tbb::spin_rw_mutex ptrs_mutex_;
    tbb::concurrent_unordered_map<bdtree::logical_pointer, std::tuple<bdtree::physical_pointer, uint64_t>,
            std::hash<bdtree::logical_pointer>> ptrs_;
};

class dummy_node_table : public bdtree::base_node_table<dummy_node_table, dummy_node_data> {
public:
    dummy_node_table()
            : counter_(0x0u) {
    }

    bdtree::physical_pointer get_next_ptr() {
        return {++counter_};
    }

    bdtree::physical_pointer get_remote_ptr() {
        return {counter_.load()};
    }

    dummy_node_data read(bdtree::physical_pointer pptr, std::error_code& ec) {
        typename decltype(nodes_mutex_)::scoped_lock _(nodes_mutex_, false);
        auto i = nodes_.find(pptr);
        if (i == nodes_.end()) {
            ec = make_error_code(bdtree::error::object_doesnt_exist);
            return dummy_node_data(std::vector<char>());
        }
        return dummy_node_data(i->second);
    }

    using bdtree::base_node_table<dummy_node_table, dummy_node_data>::read;

    void insert(bdtree::physical_pointer pptr, const char* data, size_t length, std::error_code& ec) {
        typename decltype(nodes_mutex_)::scoped_lock _(nodes_mutex_, false);
        auto res = nodes_.emplace(pptr, std::vector<char>(data, data + length));
        if (!res.second) {
            ec = make_error_code(bdtree::error::object_exists);
        }
    }

    using bdtree::base_node_table<dummy_node_table, dummy_node_data>::insert;

    void remove(bdtree::physical_pointer pptr, std::error_code& ec) {
        typename decltype(nodes_mutex_)::scoped_lock _(nodes_mutex_, true);
        auto i = nodes_.find(pptr);
        if (i == nodes_.end()) {
            ec = make_error_code(bdtree::error::object_doesnt_exist);
            return;
        }
        nodes_.unsafe_erase(i);
    }

    using bdtree::base_node_table<dummy_node_table, dummy_node_data>::remove;

private:
    std::atomic<uint64_t> counter_;

    tbb::spin_rw_mutex nodes_mutex_;
    tbb::concurrent_unordered_map<bdtree::physical_pointer, std::vector<char>, std::hash<bdtree::physical_pointer>>
            nodes_;
};

class dummy_backend : crossbow::non_copyable, crossbow::non_movable {
public:
    using ptr_table = dummy_ptr_table;

    using node_table = dummy_node_table;

    ptr_table& get_ptr_table() {
        return ptr_;
    }

    node_table& get_node_table() {
        return node_;
    }

private:
    dummy_ptr_table ptr_;
    dummy_node_table node_;
};
