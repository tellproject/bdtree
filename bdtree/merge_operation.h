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

#include <stack>

#include "base_types.h"
#include "primitive_types.h"
#include "util.h"
#include "logical_table_cache.h"
#include "deltas.h"

namespace bdtree {

template<typename Key, typename Value, typename Backend>
struct merge_operation {
public:
    typedef operation_context<Key, Value, Backend> context_t;
private:
    template<typename NodeType>
    static bool is_left_sibling(NodeType* node, logical_pointer right_lptr) {
        return node->right_link_ == right_lptr;
    }

    static node_pointer<Key, Value>* get_left_sibling(logical_pointer lptr, const Key& low_key, context_t& context, int8_t level) {
        key_compare<Key, Value> cmp;
        if (context.node_stack.size() > 1)
            context.node_stack.pop();
        for (;;) {
            auto nodep = fix_stack(low_key, context, search_bound::LAST_SMALLER);
            auto nt = nodep->node_->get_node_type();
            if (nt == node_type_t::InnerNode) {
                auto inner = nodep->as_inner();
                if (is_left_sibling(inner, lptr)) return nodep;
                if (inner->level == level) return nullptr;
                auto iter = last_smaller(inner->array_.begin(), inner->array_.end(), low_key, cmp);
                context.node_stack.push(iter->second);
            } else if (nt == node_type_t::LeafNode) {
                auto leaf = nodep->as_leaf();
                if (is_left_sibling(leaf, lptr)) return nodep;
                return nullptr;
            } else {
                assert(false);
            }
        }
        assert(false);
        return nullptr;
    }

    template<typename NodeType>
    static void consolidate(NodeType* left, NodeType* right, logical_pointer merge_lptr, physical_pointer merge_pptr, uint64_t merge_rc_version, merge_delta<Key, Value> *mergedelta, context_t& context) {
        auto& node_table = context.get_node_table();
        auto pptr = node_table.get_next_ptr();

        NodeType* consolidated = new NodeType(*left);
        consolidated->array_.reserve(consolidated->array_.size() + right->array_.size());
        consolidated->array_.insert(consolidated->array_.end(), right->array_.begin(), right->array_.end());
        consolidated->right_link_ = right->right_link_;
        consolidated->high_key_ = right->high_key_;
        consolidated->clear_deltas();
        consolidated->set_pptr(pptr);
        std::vector<uint8_t> data = consolidated->serialize();
        node_table.insert(pptr, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));

        auto& ptr_table = context.get_ptr_table();
        std::error_code ec;
        auto rc_version = ptr_table.update(merge_lptr, pptr, merge_rc_version, ec);
        if (ec == error::wrong_version || ec == error::object_doesnt_exist) {
            if (ec == error::object_doesnt_exist)
                context.cache.invalidate(merge_lptr);
            delete consolidated;
            node_table.remove(pptr);
            return;
        }
        assert(!ec);
        node_pointer<Key, Value>* np = new node_pointer<Key, Value>(merge_lptr, pptr, rc_version);
        np->node_ = consolidated;
        if (!context.cache.add_entry(np, context.tx_id)) {
            delete np;
        }
        context.cache.invalidate(mergedelta->rmdelta);
        // TODO We need to know the pointer version
        ptr_table.remove(mergedelta->rmdelta, std::numeric_limits<uint64_t>::max());
        node_table.remove(merge_pptr);
        node_table.remove(mergedelta->rmdeltapptr);
        node_delete<NodeType>::rem(*left, mergedelta->next, context);
        node_delete<NodeType>::rem(*right, mergedelta->rm_next, context);
    }

    static void consolidate(logical_pointer merge_lptr, physical_pointer merge_pptr, uint64_t merge_rc_version, merge_delta<Key, Value> *mergedelta, context_t& context) {
        auto& node_table = context.get_node_table();
        std::error_code ec;
        auto buf = node_table.read(mergedelta->next, ec);
        if (ec == error::object_doesnt_exist) {
            return;
        }
        assert(!ec);
        auto n = deserialize<Key, Value>(reinterpret_cast<const uint8_t*>(buf.data()), buf.length(), mergedelta->next);
        resolve_operation<Key, Value, Backend> op(merge_lptr, mergedelta->next, nullptr, context, merge_rc_version);
        if (!n->accept(op)) {
            return;
        }
        buf = node_table.read(mergedelta->rm_next, ec);
        if (ec == error::object_doesnt_exist) {
            return;
        }
        assert(!ec);
        n = deserialize<Key, Value>(reinterpret_cast<const uint8_t*>(buf.data()), buf.length(), mergedelta->rm_next);
        resolve_operation<Key, Value, Backend> op2(mergedelta->rmdelta, mergedelta->rm_next, nullptr, context, merge_rc_version);
        if (!n->accept(op2)) {
            return;
        }
        auto leftp = op.result;
        auto rightp = op2.result;
        auto nt = leftp->get_node_type();
        assert(nt == rightp->get_node_type());
        if (nt == node_type_t::LeafNode) {
            consolidate(static_cast<leaf_node<Key, Value>*>(leftp), static_cast<leaf_node<Key, Value>*>(rightp), merge_lptr, merge_pptr, merge_rc_version, mergedelta, context);
        } else if (nt == node_type_t::InnerNode) {
            consolidate(static_cast<inner_node<Key, Value>*>(leftp), static_cast<inner_node<Key, Value>*>(rightp), merge_lptr, merge_pptr, merge_rc_version, mergedelta, context);
        } else {
            assert(false);
        }
    }

public:

    //continue the merge from a merge delta, remove entry from parent
    static void continue_merge(logical_pointer merge_lptr, physical_pointer merge_pptr, uint64_t merge_rc_version, merge_delta<Key, Value> *mergedelta, context_t& context) {
        if (merge_lptr.value == 1) {
            consolidate(merge_lptr, merge_pptr, merge_rc_version, mergedelta, context);
            return;
        }
        auto& node_table = context.get_node_table();
        key_compare<Key, Value> cmp;
        if (context.node_stack.size() > 1)
            context.node_stack.pop();
        for (;;) {
            auto parent = fix_stack(mergedelta->right_low_key, context, search_bound::LAST_SMALLER_EQUAL);
            auto ntype = parent->node_->get_node_type();
            if (ntype == node_type_t::LeafNode) {
                consolidate(merge_lptr, merge_pptr, merge_rc_version, mergedelta, context);
                return;
            }
            auto inner = parent->as_inner();
            auto iter = last_smaller_equal(inner->array_.begin(), inner->array_.end(), mergedelta->right_low_key, cmp);
            if (iter == inner->array_.end()) {
                assert(parent->lptr_.value == 1);//root node
                return;
            }
            if (iter->first == mergedelta->right_low_key) {
                if (iter->second == mergedelta->rmdelta) {
                    auto& ptr_table = context.get_ptr_table();

                    size_t nsize = inner->serialized_size();
                    if (nsize < MIN_NODE_SIZE && parent->lptr_.value != 1) {
                        //merge
                        merge(parent, context);
                        continue;
                    }
                    if (parent->lptr_.value == 1 && inner->array_.size() == 2) {
                        //decrease tree depth
                        std::error_code ec;
                        auto rc_version = ptr_table.update(parent->lptr_, merge_pptr, parent->rc_version_, ec);
                        if (ec == error::wrong_version || ec == error::object_doesnt_exist) {
                            if (ec == error::wrong_version) {
                                context.cache.invalidate_if_older(parent->lptr_, rc_version);
                            }
                            else {
                                context.cache.invalidate(parent->lptr_);
                            }
                            continue;
                        }
                        assert(!ec);
                        node_table.remove(parent->ptr_);
                        ptr_table.remove(merge_lptr, std::numeric_limits<uint64_t>::max());
                        consolidate(parent->lptr_, merge_pptr, rc_version, mergedelta, context);
                        return;
                    }
                    inner_node<Key, Value> newinner(*inner);
                    auto rm_offset = iter - inner->array_.begin();
                    newinner.array_.erase(newinner.array_.begin() + rm_offset);
                    std::vector<uint8_t> data = newinner.serialize();

                    auto pptr = node_table.get_next_ptr();
                    node_table.insert(pptr, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));

                    uint64_t rc_version;
                    std::error_code ec;
                    if (rm_offset != 0) {
                        rc_version = ptr_table.update(parent->lptr_, pptr, parent->rc_version_, ec);
                    }
                    else {
                        //trigger parent merge
                        remove_delta<Key, Value> rm_delta;
                        rm_delta.next = pptr;
                        rm_delta.low_key = newinner.low_key_;
                        rm_delta.level = newinner.level;
                        auto data = rm_delta.serialize();

                        auto rm_pptr = node_table.get_next_ptr();
                        node_table.insert(rm_pptr, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));

                        rc_version = ptr_table.update(parent->lptr_, rm_pptr, parent->rc_version_, ec);
                        if (ec) {
                            node_table.remove(rm_pptr);
                        }
                    }
                    if (ec == error::wrong_version || ec == error::object_doesnt_exist) {
                        if (ec == error::wrong_version) {
                            context.cache.invalidate_if_older(parent->lptr_, rc_version);
                        }
                        else {
                            context.cache.invalidate(parent->lptr_);
                        }
                        node_table.remove(pptr);
                        continue;
                    }
                    assert(!ec);
                    node_table.remove(parent->ptr_);
                    consolidate(merge_lptr, merge_pptr, merge_rc_version, mergedelta, context);
                    return;
                }
            }
            if (iter->second == merge_lptr) {
                return;
            }
            context.node_stack.push(iter->second);
        }
    }

    //continue the merge from a remove delta
    static void continue_merge(logical_pointer removedelta_lptr, physical_pointer removedelta_pptr, uint64_t rm_rc_version, remove_delta<Key, Value> *rmdelta, operation_context<Key, Value, Backend>& context) {
        Key low_key = rmdelta->low_key;
        auto leftp = get_left_sibling(removedelta_lptr, low_key, context, rmdelta->level);
        if (leftp == nullptr) {
            return;
        }
        merge_delta<Key, Value> merge;
        merge.right_low_key = low_key;
        merge.rmdelta = removedelta_lptr;
        merge.rm_next = rmdelta->next;
        merge.rmdeltapptr = removedelta_pptr;
        merge.level = rmdelta->level;
        std::vector<uint8_t> data;
        auto& node_table = context.get_node_table();
        auto& ptr_table = context.get_ptr_table();
        for (;;) {
            auto merge_pptr = node_table.get_next_ptr();
            merge.next = leftp->ptr_;
            data = merge.serialize();
            node_table.insert(merge_pptr, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));

            std::error_code ec;
            auto merge_rc_version = ptr_table.update(leftp->lptr_, merge_pptr, leftp->rc_version_, ec);
            if (ec == error::wrong_version || ec == error::object_doesnt_exist) {
                if (ec == error::object_doesnt_exist) {
                    context.cache.invalidate(leftp->lptr_);
                }
                leftp = fix_stack(low_key, context, search_bound::LAST_SMALLER);
                auto nt = leftp->get_node_type();
                if (nt == node_type_t::InnerNode) {
                    auto inner = leftp->as_inner();
                    if (is_left_sibling(inner, removedelta_lptr)) {
                        continue;
                    }
                } else if (nt == node_type_t::LeafNode) {
                    auto leaf = leftp->as_leaf();
                    if (is_left_sibling(leaf, removedelta_lptr)) {
                        continue;
                    }
                }
                node_table.remove(merge_pptr);
                return;
            }
            assert(!ec);
            continue_merge(leftp->lptr_, merge_pptr, merge_rc_version, &merge, context);
            return;
        }
    }

    template<typename NodeType>
    static void execute_merge(node_pointer<Key, Value>* nodep, NodeType* to_merge, context_t& context) {
        assert(nodep->node_ == to_merge);
        if (to_merge->low_key_ == null_key<Key>::value()) {
            // if we are at the left most node, we merge the right node
            assert(to_merge->high_key_);
            auto old_nodep = nodep;
            nodep = context.cache.get_without_cache(to_merge->right_link_, context);
            if (nodep == nullptr) {
                //make sure this version is never read again
                context.cache.invalidate_if_older(old_nodep->lptr_, old_nodep->rc_version_ + 1);
                return;
            }
            merge(nodep, context);
            return;
        }
        remove_delta<Key, Value> rmdelta;
        rmdelta.low_key = to_merge->low_key_;
        rmdelta.next = nodep->ptr_;
        rmdelta.level = to_merge->level;
        std::vector<uint8_t> data = rmdelta.serialize();

        auto& node_table = context.get_node_table();
        auto pptr = node_table.get_next_ptr();
        node_table.insert(pptr, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));

        auto& ptr_table = context.get_ptr_table();
        std::error_code ec;
        auto rc_version = ptr_table.update(nodep->lptr_, pptr, nodep->rc_version_, ec);
        if (ec == error::wrong_version || ec == error::object_doesnt_exist) {
            if (ec == error::wrong_version) {
                context.cache.invalidate_if_older(nodep->lptr_, rc_version);
            } else {
                context.cache.invalidate(nodep->lptr_);
            }
            node_table.remove(pptr);
            return;
        }
        assert(!ec);
        continue_merge(nodep->lptr_, pptr, rc_version, &rmdelta, context);
    }

    static void merge(node_pointer<Key, Value>* node, context_t& context) {
        node_type_t nt = node->node_->get_node_type();
        if (nt == node_type_t::LeafNode) {
            execute_merge(node, node->as_leaf(), context);
        } else if (nt == node_type_t::InnerNode) {
            execute_merge(node, node->as_inner(), context);
        }
    }
};

}
