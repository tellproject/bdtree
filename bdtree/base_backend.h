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

#include <bdtree/base_types.h>

#include <system_error>
#include <tuple>

namespace bdtree {
namespace detail {

inline void throw_error(const std::error_code& ec) {
    if (ec) {
        throw std::system_error(ec);
    }
}

} // namespace detail

template <typename HandlerType>
class base_ptr_table {
public:
    std::tuple<physical_pointer, uint64_t> read(logical_pointer lptr);

    uint64_t insert(logical_pointer lptr, physical_pointer pptr);

    uint64_t update(logical_pointer lptr, physical_pointer pptr, uint64_t version);

    void remove(logical_pointer lptr, uint64_t version);
};

template <typename HandlerType>
std::tuple<physical_pointer, uint64_t> base_ptr_table<HandlerType>::read(logical_pointer lptr) {
    std::error_code ec;
    auto tuple = static_cast<HandlerType*>(this)->read(lptr, ec);
    detail::throw_error(ec);
    return tuple;
}

template <typename HandlerType>
uint64_t base_ptr_table<HandlerType>::insert(logical_pointer lptr, physical_pointer pptr) {
    std::error_code ec;
    auto version = static_cast<HandlerType*>(this)->insert(lptr, pptr, ec);
    detail::throw_error(ec);
    return version;
}

template <typename HandlerType>
uint64_t base_ptr_table<HandlerType>::update(logical_pointer lptr, physical_pointer pptr, uint64_t version) {
    std::error_code ec;
    auto rc_version = static_cast<HandlerType*>(this)->update(lptr, pptr, version, ec);
    detail::throw_error(ec);
    return rc_version;
}

template <typename HandlerType>
void base_ptr_table<HandlerType>::remove(logical_pointer lptr, uint64_t version) {
    std::error_code ec;
    static_cast<HandlerType*>(this)->remove(lptr, version, ec);
    detail::throw_error(ec);
}

template <typename HandlerType, typename DataType>
class base_node_table {
public:
    DataType read(physical_pointer pptr);

    void insert(physical_pointer pptr, const char* data, size_t length);

    void remove(physical_pointer pptr);
};

template <typename HandlerType, typename DataType>
DataType base_node_table<HandlerType, DataType>::read(physical_pointer pptr) {
    std::error_code ec;
    auto data = static_cast<HandlerType*>(this)->read(pptr, ec);
    detail::throw_error(ec);
    return data;
}

template <typename HandlerType, typename DataType>
void base_node_table<HandlerType, DataType>::insert(physical_pointer pptr, const char* data, size_t length) {
    std::error_code ec;
    static_cast<HandlerType*>(this)->insert(pptr, data, length, ec);
    detail::throw_error(ec);
}

template <typename HandlerType, typename DataType>
void base_node_table<HandlerType, DataType>::remove(physical_pointer pptr) {
    std::error_code ec;
    static_cast<HandlerType*>(this)->remove(pptr, ec);
    detail::throw_error(ec);
}

} // namespace bdtree
