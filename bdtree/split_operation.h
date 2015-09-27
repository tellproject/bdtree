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

#include <bdtree/deltas.h>
#include <bdtree/logical_table_cache.h>
#include <bdtree/primitive_types.h>
#include <bdtree/util.h>
#include <bdtree/error_code.h>

#include <functional>
#include <mutex>
#include <stack>

namespace bdtree {

template<typename Key, typename Value, typename Backend>
struct split_operation {
private:

    template<typename NodeType, typename Func>
    static void consolidate_typed(NodeType* iorl, operation_context<Key, Value, Backend>& context,
            split_delta<Key, Value>* delta, logical_pointer split_lptr, physical_pointer split_pptr,
            uint64_t lptr_version, Func fun) {
        NodeType* consolidated = new NodeType(*iorl);
        consolidated->array_.erase(consolidated->array_.begin() + consolidated->array_.size()/2, consolidated->array_.end());
        consolidated->right_link_ = delta->new_right;
        consolidated->high_key_ = delta->right_key;
        consolidated->clear_deltas();
        auto data = consolidated->serialize();

        auto& node_table = context.get_node_table();
        auto pptr = node_table.get_next_ptr();
        node_table.insert(pptr, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));

        auto& ptr_table = context.get_ptr_table();
        std::error_code ec;
        auto new_lptr_version = ptr_table.update(split_lptr, pptr, lptr_version, ec);
        if (!ec) {
            node_pointer<Key, Value>* nodep = new node_pointer<Key, Value>(split_lptr, pptr, new_lptr_version);
            consolidated->set_pptr(pptr);
            nodep->node_ = consolidated;
            if (!context.cache.add_entry(nodep, context.tx_id)) {
                delete nodep;
            }
            node_table.remove(split_pptr);
            fun(iorl);
            return;
        } else if (ec == error::wrong_version || ec == error::object_doesnt_exist) {
            delete consolidated;
            node_table.remove(pptr);
        }
    }

    static void consolidate_split(logical_pointer split_lptr, physical_pointer split_pptr,
            split_delta<Key, Value>* delta, uint64_t lptr_version, operation_context<Key, Value, Backend>& context) {
        auto& node_table = context.get_node_table();
        std::error_code ec;
        auto buf = node_table.read(delta->next, ec);
        if (ec == error::object_doesnt_exist) {
            return;
        }
        assert(!ec);
        auto n = deserialize<Key, Value>(reinterpret_cast<const uint8_t*>(buf.data()), buf.length(), delta->next);
        resolve_operation<Key, Value, Backend> op(split_lptr, delta->next, nullptr, context, lptr_version);
        if (!n->accept(op)) {
            return;
        }
        auto nt = op.result->get_node_type();
        if (nt == node_type_t::InnerNode) {
            inner_node<Key, Value>* inner = static_cast<inner_node<Key, Value>*>(op.result);
            consolidate_typed(inner, context, delta, split_lptr, split_pptr, lptr_version, [&context, delta]
                    (inner_node<Key, Value>*) {
                context.get_node_table().remove(delta->next);
            });
        } else if (nt == node_type_t::LeafNode) {
            leaf_node<Key, Value>* leaf = static_cast<leaf_node<Key, Value>*>(op.result);
            consolidate_typed(leaf, context, delta, split_lptr, split_pptr, lptr_version, [&context, leaf]
                    (leaf_node<Key, Value>* n) {
                auto& node_table = context.get_node_table();
                node_table.remove(leaf->leaf_pptr_);
                for (physical_pointer ptr : n->deltas_) {
                    node_table.remove(ptr);
                }
            });
        } else {
            assert(false);
        }
    }

public:
    static void continue_split(logical_pointer split_lptr, physical_pointer split_pptr, uint64_t split_rc_version,
            split_delta<Key, Value> *delta, operation_context<Key, Value, Backend>& context) {
        assert(context.node_stack.size() >= 2);
        key_compare<Key, Value> cmp;
        context.node_stack.pop();
        auto& node_table = context.get_node_table();
        for (;;) {
            node_pointer<Key, Value> *parent = fix_stack(delta->right_key, context, search_bound::LAST_SMALLER_EQUAL);
            auto ntype = parent->node_->get_node_type();
            if (ntype == node_type_t::LeafNode) {
                return;
            } else if (ntype != node_type_t::InnerNode) {
                assert(false);
            }
            inner_node<Key, Value> *inner = static_cast<inner_node<Key, Value>*>(parent->node_);
            auto iter = last_smaller_equal(inner->array_.begin(), inner->array_.end(), delta->right_key, cmp);
            assert(iter != inner->array_.end());
            if (iter->first == delta->right_key) {
                consolidate_split(split_lptr, split_pptr, delta, split_rc_version, context);
                return;
            }
            if (iter->second != split_lptr) {
                context.node_stack.push(iter->second);
                continue;
            }

            auto& ptr_table = context.get_ptr_table();
            std::error_code ec;
            auto actual_split_pptr = ptr_table.read(split_lptr, ec);
            if (ec == error::object_doesnt_exist) {
                return;
            }
            assert(!ec);
            if (split_pptr != std::get<0>(actual_split_pptr)) {
                return;
            }
            size_t nsize = inner->serialized_size();
            if (nsize >= MAX_NODE_SIZE) {
                //split
                split(parent, context);
                continue;
            }
            inner_node<Key, Value>* new_inner = new inner_node<Key, Value>(*inner);
            auto i = iter - inner->array_.begin() + 1;//insert after the next smaller
            new_inner->array_.insert(new_inner->array_.begin() + i, std::make_pair(delta->right_key, delta->new_right));
            auto data = new_inner->serialize();

            auto pptr = node_table.get_next_ptr();
            node_table.insert(pptr, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));

            auto lptr_version = ptr_table.update(parent->lptr_, pptr, parent->rc_version_, ec);
            if (!ec) {
                consolidate_split(split_lptr, split_pptr, delta, split_rc_version, context);
                node_table.remove(parent->ptr_);
                auto* nnp = new node_pointer<Key, Value>(parent->lptr_, pptr, lptr_version);
                nnp->node_ = new_inner;
                if (!context.cache.add_entry(nnp, context.tx_id)) {
                    delete nnp;
                }
                return;
            } else if (ec == error::wrong_version) {
                delete new_inner;
            } else if (ec == error::object_doesnt_exist) {
                delete new_inner;
                context.cache.invalidate(parent->lptr_);
            }
            node_table.remove(pptr);
        }
        assert(false);
    }

    template<typename NodeType>
    static void execute_split(node_pointer<Key, Value>* nodep, NodeType* to_split,
            operation_context<Key, Value, Backend>& context) {
        assert(nodep->node_ == to_split);
        auto& node_table = context.get_node_table();
        auto right_pptr = node_table.get_next_ptr();
        auto split_ptr = node_table.get_next_ptr();
        auto& ptr_table = context.get_ptr_table();
        auto right_lptr = ptr_table.get_next_ptr();

        auto right_lptr_version = ptr_table.insert(right_lptr, right_pptr);

        NodeType* right = new NodeType(right_pptr);
        right->array_.insert(right->array_.begin(), to_split->array_.begin() + to_split->array_.size()/2, to_split->array_.end());
        right->high_key_ = to_split->high_key_;
        right->low_key_ = right->array_.front().first;
        right->right_link_ = to_split->right_link_;
        right->set_level(to_split->level);
        assert(!to_split->high_key_ || *to_split->high_key_ == *right->high_key_);
        std::vector<uint8_t> data = right->serialize();
        node_table.insert(right_pptr, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));
        if (nodep->lptr_.value == 1) {
            // root split
            assert(to_split->low_key_ == null_key<Key>::value() && !to_split->high_key_);
            logical_pointer lptr_left = ptr_table.get_next_ptr();
            physical_pointer pptr_left = node_table.get_next_ptr();
            physical_pointer pptr_newroot = node_table.get_next_ptr();
            auto lptr_left_version = ptr_table.insert(lptr_left, pptr_left);

            node_pointer<Key, Value> *leftp = new node_pointer<Key, Value>(lptr_left, pptr_left, lptr_left_version);
            inner_node<Key, Value> *new_root = new inner_node<Key, Value>(pptr_left);
            new_root->low_key_ = null_key<Key>::value();
            new_root->array_.reserve(2);
            new_root->array_.push_back(std::make_pair(null_key<Key>::value(), lptr_left));
            new_root->array_.push_back(std::make_pair(right->low_key_, right_lptr));
            new_root->set_level(int8_t(to_split->level + 1));
            data = new_root->serialize();
            node_table.insert(pptr_newroot, reinterpret_cast<const char *>(data.data()), uint32_t(data.size()));

            NodeType *left = new NodeType(pptr_left);
            left->array_.insert(left->array_.begin(), to_split->array_.begin(), to_split->array_.begin() + to_split->array_.size()/2);
            left->low_key_ = to_split->low_key_;
            left->high_key_ = right->low_key_;
            left->right_link_ = right_lptr;
            left->set_level(to_split->level);
            leftp->node_ = left;
            data = left->serialize();
            node_table.insert(pptr_left, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));

            std::lock_guard<std::mutex> _(nodep->mutex_);
            std::error_code ec;
            auto rc_version = ptr_table.update(nodep->lptr_, pptr_newroot, nodep->rc_version_, ec);
            assert(ec != error::object_doesnt_exist); //the root node must always exist
            if (!ec) {
                node_pointer<Key, Value>* rightp = new node_pointer<Key, Value>(right_lptr, right_pptr, right_lptr_version);
                node_pointer<Key, Value>* rootp = new node_pointer<Key, Value>(nodep->lptr_, pptr_newroot, rc_version);
                rootp->node_ = new_root;
                rightp->node_ = right;
                if (!context.cache.add_entry(rootp, context.tx_id)) {
                    delete rootp;
                }
                if (!context.cache.add_entry(rightp, context.tx_id)) {
                    delete rightp;
                }
                if (!context.cache.add_entry(leftp, context.tx_id)) {
                    delete leftp;
                }
                //delete old pptr
                node_delete<NodeType>::rem(*to_split, nodep->ptr_, context);
                return;
            } else if (ec == error::wrong_version) {
                context.cache.invalidate_if_older(nodep->lptr_, rc_version);
                ptr_table.remove(right_lptr, right_lptr_version);
                node_table.remove(right_pptr);
                ptr_table.remove(lptr_left, lptr_left_version);
                node_table.remove(pptr_newroot);
                node_table.remove(pptr_left);
                delete right;
                delete leftp;
                delete new_root;
                return;
            } else {
                assert(false);
            }
            assert(false);
            return;
        }
        split_delta<Key, Value> *split = new split_delta<Key, Value>();
        split->next = nodep->ptr_;
        split->new_right = right_lptr;
        split->right_key = right->low_key_;
        split->level = right->level;
        data = split->serialize();
        node_table.insert(split_ptr, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));
        std::error_code ec;
        auto rc_version = ptr_table.update(nodep->lptr_, split_ptr, nodep->rc_version_, ec);
        delete right;
        if (!ec) {
            continue_split(nodep->lptr_, split_ptr, rc_version, split, context);
            delete split;
        } else if (ec == error::wrong_version || ec == error::object_doesnt_exist) {
            if (ec == error::wrong_version) {
                context.cache.invalidate_if_older(nodep->lptr_, rc_version);
            } else {
                context.cache.invalidate(nodep->lptr_);
            }
            ptr_table.remove(right_lptr, right_lptr_version);
            node_table.remove(right_pptr);
            node_table.remove(split_ptr);
            delete split;
        } else {
            assert(false);
        }
    }

    static void split(node_pointer<Key, Value>* node, operation_context<Key, Value, Backend>& context) {
        node_type_t nt = node->node_->get_node_type();
        if (nt == node_type_t::LeafNode) {
            execute_split(node, node->as_leaf(), context);
        } else if (nt == node_type_t::InnerNode) {
            execute_split(node, node->as_inner(), context);
        }
    }
};

}
