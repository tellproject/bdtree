#pragma once
#include "amalloc.h"
#include <array>
#include <limits>
#include <mutex>
#include <vector>
#include <stack>

#include <cramcloud.h>
#include <concurrent_map.h>

#include "bdtree/serialize_policies.h"
#include "bdtree/primitive_types.h"
#include "bdtree/forward_declarations.h"
#include "bdtree/base_types.h"
#include "bdtree/stl_specializations.h"
#include "bdtree/node_pointer.h"
#include "bdtree/logical_table_cache.h"
#include "bdtree/deltas.h"
#include "bdtree/nodes.h"
#include "bdtree/resolve_operation.h"
#include "bdtree/iterator.h"
#include "bdtree/search_operation.h"
#include "bdtree/leaf_operations.h"


namespace bdtree {

    struct empty_t {
        bool operator==(const empty_t &) {return true;}
        bool operator!=(const empty_t &) {return false;}
    };
    
    template<typename Key, typename Value>
    class map
    {
        logical_table_cache<Key, Value>& cache_;
        uint64_t tx_id_;
    public: // types
        typedef Key key_type;
        typedef Value value_type;
        typedef bdtree::erase_result erase_result;
        typedef bdtree_iterator<Key, Value> iterator;
    public: // construction/destruction
        map(logical_table_cache<Key, Value>& cache, uint64_t tx_id, bool init = false)
            : cache_(cache), tx_id_(tx_id)
        {
            if (init) {
                // root node
                physical_pointer one_pptr{1};
                logical_pointer one_lptr{1};
                ramcloud_reject_rules r;
                r.exists = 1;
                //write 1 into node counter
                counter node_counter(cache_.get_node_table().value);
                node_counter.init(1);
                //write 1 into lptr counter
                counter ptr_counter(cache_.get_ptr_table().value);
                ptr_counter.init(1);
                leaf_node<Key, Value>* node = new leaf_node<Key, Value>(one_pptr);
                node->low_key_ = null_key<Key>::value();
                auto ser = node->serialize();
                //write root node at pptr{1}
                auto err = rc_write_with_reject(cache_.get_node_table().value, one_pptr.value_ptr(), one_pptr.length, reinterpret_cast<const char*>(ser.data()), uint32_t(ser.size()),&ramcloud_reject_if_exists, nullptr);
                if (err != STATUS_OK){
                    assert(err == STATUS_OBJECT_EXISTS);
                    delete node;
                    return;
                }

                // ptr to root (lptr{1} -> pptr{1})
                uint64_t rc_version;
                auto last_tx_id = get_last_tx_id();
                err = rc_write_with_reject(cache_.get_ptr_table().value, one_lptr.value_ptr(), one_lptr.length, (const char*) &one_pptr, uint32_t(sizeof(one_pptr)), nullptr, &rc_version);
                assert(err == STATUS_OK);
                node_pointer<Key, Value> *nptr = new node_pointer<Key, Value>(one_lptr, one_pptr, rc_version);
                nptr->node_ = node;
                if (!cache_.add_entry(nptr, last_tx_id)) {
                    delete nptr;
                }
            }
        }
    public: // operations
        iterator find(const key_type& key) const {
            return lower_bound(key, cache_, tx_id_);
        }
        iterator end() const {
            return iterator();
        }

        bool insert(const Key& key, const Value& value) {
            key_compare<Key, Value> comp;
            insert_operation<Key, Value> op(key, value, comp);
            return exec_leaf_operation(key, cache_, tx_id_, op);
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
            return exec_leaf_operation(key, cache_, tx_id_, op);
        }
        
        bool remove_if_unmodified(iterator& iter) {
            return iter.erase_if_no_newer() == erase_result::Success;
        }

        void print_statistics() {
            ramcloud_buffer buf;
            counter node_counter(cache_.get_node_table().value);
            uint64_t max_node = node_counter.get_remote_value();
            std::vector<uint64_t> counts(uint8_t(node_type_t::MergeDelta) + 1);
            for (uint64_t i = 1; i <= max_node ; ++i) {
                physical_pointer pptr{i};
                auto rc_res = rc_read(cache_.get_node_table().value, pptr.value_ptr(), pptr.length, &buf);
                if (rc_res != STATUS_OK)
                    continue;
                auto* node = deserialize<Key, Value>(buf.data, buf.length, pptr);
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
}
