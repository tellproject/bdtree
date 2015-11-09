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

#include <crossbow/Serializer.hpp>
#include <bdtree/primitive_types.h>
#include <bdtree/forward_declarations.h>
#include <bdtree/base_types.h>
#include <bdtree/stl_specializations.h>
#include <bdtree/node_pointer.h>
#include <bdtree/logical_table_cache.h>
#include <bdtree/deltas.h>
#include <bdtree/nodes.h>
#include <bdtree/resolve_operation.h>
#include <bdtree/iterator.h>
#include <bdtree/search_operation.h>
#include <bdtree/leaf_operations.h>
#include <bdtree/error_code.h>

#include <array>
#include <limits>
#include <mutex>
#include <vector>

namespace bdtree {

    struct empty_t {
        bool operator==(const empty_t &) {return true;}
        bool operator!=(const empty_t &) {return false;}
    };

    template<typename Key, typename Value, typename Backend>
    class map
    {
        Backend& backend_;
        logical_table_cache<Key, Value, Backend>& cache_;
        uint64_t tx_id_;
    public: // types
        typedef Key key_type;
        typedef Value value_type;
        typedef bdtree::erase_result erase_result;
        typedef bdtree_iterator<Key, Value, Backend> iterator;
    public: // construction/destruction
        map(Backend& backend, logical_table_cache<Key, Value, Backend>& cache, uint64_t tx_id, bool doInit = false)
            : backend_(backend), cache_(cache), tx_id_(tx_id)
        {
            if (doInit) {
                init(backend_, cache_);
            }
        }

    public: // operations
        iterator find(const key_type& key) const {
            return lower_bound(key, backend_, cache_, tx_id_);
        }
        iterator end() const {
            return iterator();
        }

        bool insert(const Key& key, const Value& value) {
            key_compare<Key, Value> comp;
            insert_operation<Key, Value> op(key, value, comp);
            return exec_leaf_operation(key, backend_, cache_, tx_id_, op);
        }

        template <typename V = Value>
        typename std::enable_if<std::is_same<empty_t, V>::value, bool>::type insert(const Key& key) {
            return insert(key, empty_t{});
        }
        template <typename V = Value>
        typename std::enable_if<!std::is_same<empty_t, V>::value, bool>::type insert(const std::pair<Key, Value>& p) {
            return insert(p.first, p.second);
        }
        bool erase(const Key& key) {
            key_compare<Key, Value> comp;
            delete_operation<Key, Value> op(key, comp);
            return exec_leaf_operation(key, backend_, cache_, tx_id_, op);
        }

        //bool remove_if_unmodified(iterator& iter) {
        //    return iter.erase_if_no_newer() == erase_result::Success;
        //}

        void print_statistics() {
            auto& node_table = backend_.get_node_table();
            uint64_t max_node = node_table.get_remote_ptr().value;
            std::vector<uint64_t> counts(uint8_t(node_type_t::MergeDelta) + 1);
            for (uint64_t i = 1; i <= max_node ; ++i) {
                physical_pointer pptr{i};
                std::error_code ec;
                auto buf = node_table.read(pptr, ec);
                if (ec)
                    continue;
                auto* node = deserialize<Key, Value>(reinterpret_cast<const uint8_t*>(buf.data()), buf.length(), pptr);
                if (node->get_node_type() == node_type_t::LeafNode) {
                    std::cout << "found leaf_node with pptr: " << pptr.value << std::endl;
                } else if (node->get_node_type() == node_type_t::InsertDelta) {
                    auto* ins_delta = reinterpret_cast<insert_delta<Key,Value>*>(node);
                    std::cout << "found insert_delta with pptr: " << pptr.value << " pointing to " << ins_delta->next.value << std::endl;
                }
                counts[uint8_t(node->get_node_type())]++;
                delete node;
            }
            auto iter = find(null_key<Key>::value());
            auto* last_current = iter.current_;
            uint64_t leaf_nodes_in_use = 0;
            while (last_current) {
                if (iter.current_ != last_current){
                    std::cout << "leaf in use: " << last_current->lptr_.value << " with pptr " << last_current->ptr_.value << std::endl;
                    ++leaf_nodes_in_use;
                    last_current = iter.current_;
                }
                else
                    ++iter;
            }
//            node_pointer<Key, Value>* leftmost = iter.current_;
//            uint64_t leaf_nodes_in_use = leftmost != nullptr;
//            while (leftmost && leftmost->as_leaf()->right_link_.value != 0) {
//                leftmost = cache_.get_without_cache(leftmost->as_leaf()->right_link_);
//                auto lptr = leftmost->right_link_;
//                auto rc_res = rc_read(cache_.get_ptr_table().value, lptr.value_ptr(), lptr.length, &buf);
//                assert(rc_res == STATUS_OK);
//                physical_pointer pptr{*reinterpret_cast<uint64_t*>(buf.data)};
//                auto err = rc_read(cache_.get_node_table().value, pptr.value_ptr(), physical_pointer::length, &buf);
//                assert(err == STATUS_OK);
//                auto n = deserialize<Key, Value>(buf.data, buf.length, pptr);
//                operation_context<Key, Value> context{cache_, tx_id_};
//                resolve_operation<Key, Value> op(lptr, pptr, nullptr, context, 0);
//                if (n->accept(op)) {
//                    leftmost = op.result.release();
//                    ++leaf_nodes_in_use;
//                }
//                else {
//                    assert(false);
//                }
//            }

            for (size_t i = 0; i < counts.size() ; ++i) {
                std::cout << "NodeType " << i << " count: " << counts[i] << std::endl;
            }

            std::cout << "leaf nodes in use: " << leaf_nodes_in_use << std::endl;
        }
    };

    template<typename Key, typename Value, typename Backend>
    void init(Backend& backend, logical_table_cache<Key, Value, Backend>& cache) {
        auto& node_table = backend.get_node_table();
        auto root_pptr = node_table.get_next_ptr();
        assert(root_pptr == physical_pointer{1});
        leaf_node<Key, Value>* node = new leaf_node<Key, Value>(root_pptr);
        node->low_key_ = null_key<Key>::value();
        auto ser = node->serialize();

        //write root node at pptr{1}
        std::error_code ec;
        node_table.insert(root_pptr, reinterpret_cast<const char*>(ser.data()), ser.size(), ec);
        if (ec) {
            assert(ec == error::object_exists);
            delete node;
            return;
        }

        // ptr to root (lptr{1} -> pptr{1})
        auto last_tx_id = get_last_tx_id();

        auto& ptr_table = backend.get_ptr_table();
        auto root_lptr = ptr_table.get_next_ptr();
        assert(root_lptr == logical_pointer{1});
        auto new_lptr_version = ptr_table.insert(root_lptr, root_pptr);
        node_pointer<Key, Value> *nptr = new node_pointer<Key, Value>(root_lptr, root_pptr, new_lptr_version);
        nptr->node_ = node;
        if (!cache.add_entry(nptr, last_tx_id)) {
            delete nptr;
        }
    }
}
