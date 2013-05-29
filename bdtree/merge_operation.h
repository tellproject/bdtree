#pragma once

#include <stack>

#include "base_types.h"
#include "primitive_types.h"
#include "util.h"
#include "logical_table_cache.h"
#include "deltas.h"

namespace bdtree {

template<typename Key, typename Value>
struct merge_operation {
public:
    typedef operation_context<Key, Value> context_t;
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
        NodeType* consolidated = new NodeType(*left);
        consolidated->array_.reserve(consolidated->array_.size() + right->array_.size());
        consolidated->array_.insert(consolidated->array_.end(), right->array_.begin(), right->array_.end());
        consolidated->right_link_ = right->right_link_;
        consolidated->high_key_ = right->high_key_;
        consolidated->clear_deltas();
        physical_pointer pptr = context.cache.get_next_physical_ptr();
        consolidated->set_pptr(pptr);
        std::vector<uint8_t> data = consolidated->serialize();
        auto rc_res = rc_write(context.get_node_table().value, pptr.value_ptr(), pptr.length, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));
        assert(rc_res == STATUS_OK);
        ramcloud_reject_rules rules;
        rules.doesntExist = 1;
        rules.givenVersion = merge_rc_version;
        rules.versionNeGiven = 1;
        uint64_t rc_version;
        rc_res = rc_write_with_reject(context.get_ptr_table().value, merge_lptr.value_ptr(), merge_lptr.length, pptr.value_ptr(), pptr.length, &rules, &rc_version);
        if (rc_res == STATUS_WRONG_VERSION || rc_res == STATUS_OBJECT_DOESNT_EXIST) {
            if (rc_res == STATUS_OBJECT_DOESNT_EXIST)
                context.cache.invalidate(merge_lptr);
            delete consolidated;
            rc_res = rc_remove(context.get_node_table().value, pptr.value_ptr(), pptr.length);//save
            assert(rc_res == STATUS_OK);
            return;
        }
        assert(rc_res == STATUS_OK);
        node_pointer<Key, Value>* np = new node_pointer<Key, Value>(merge_lptr, pptr, rc_version);
        np->node_ = consolidated;
        if (!context.cache.add_entry(np, context.tx_id)) {
            delete np;
        }
        context.cache.invalidate(mergedelta->rmdelta);
        rc_res = rc_remove(context.get_ptr_table().value, mergedelta->rmdelta.value_ptr(), mergedelta->rmdelta.length);
        assert(rc_res == STATUS_OK);
        rc_res = rc_remove(context.get_node_table().value, merge_pptr.value_ptr(), merge_pptr.length);
        assert(rc_res == STATUS_OK);
        rc_res = rc_remove(context.get_node_table().value, mergedelta->rmdeltapptr.value_ptr(), mergedelta->rmdeltapptr.length);
        assert(rc_res == STATUS_OK);
        node_delete<NodeType>::rem(*left, mergedelta->next, context);
        node_delete<NodeType>::rem(*right, mergedelta->rm_next, context);
    }

    static void consolidate(logical_pointer merge_lptr, physical_pointer merge_pptr, uint64_t merge_rc_version, merge_delta<Key, Value> *mergedelta, context_t& context) {
        ramcloud_buffer buf;
        auto rc_res = rc_read(context.get_node_table().value, mergedelta->next.value_ptr(), mergedelta->next.length, &buf);
        if (rc_res == STATUS_OBJECT_DOESNT_EXIST) {
            return;
        }
        assert(rc_res == STATUS_OK);
        auto n = deserialize<Key, Value>(buf.data, buf.length, mergedelta->next);
        resolve_operation<Key, Value> op(merge_lptr, mergedelta->next, nullptr, context, merge_rc_version);
        if (!n->accept(op)) {
            return;
        }
        rc_res = rc_read(context.get_node_table().value, mergedelta->rm_next.value_ptr(), mergedelta->rm_next.length, &buf);
        if (rc_res == STATUS_OBJECT_DOESNT_EXIST) {
            return;
        }
        assert(rc_res == STATUS_OK);
        n = deserialize<Key, Value>(buf.data, buf.length, mergedelta->rm_next);
        resolve_operation<Key, Value> op2(mergedelta->rmdelta, mergedelta->rm_next, nullptr, context, merge_rc_version);
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
        physical_pointer pptr = context.cache.get_next_physical_ptr();
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

                    size_t nsize = inner->serialized_size();
                    if (nsize < MIN_NODE_SIZE && parent->lptr_.value != 1) {
                        //merge
                        merge(parent, context);
                        continue;
                    }
                    if (parent->lptr_.value == 1 && inner->array_.size() == 2) {
                        //decrease tree depth
                        uint64_t rc_version;
                        ramcloud_reject_rules rules;
                        rules.doesntExist = 1;
                        rules.givenVersion = parent->rc_version_;
                        rules.versionNeGiven = 1;
                        auto rc_res = rc_write_with_reject(context.get_ptr_table().value, parent->lptr_.value_ptr(), parent->lptr_.length, merge_pptr.value_ptr(), merge_pptr.length, &rules, &rc_version);
                        if (rc_res == STATUS_WRONG_VERSION || rc_res == STATUS_OBJECT_DOESNT_EXIST) {
                            if (rc_res == STATUS_WRONG_VERSION) {
                                context.cache.invalidate_if_older(parent->lptr_, rc_version);
                            }
                            else {
                                context.cache.invalidate(parent->lptr_);
                            }
                            continue;
                        }
                        assert(rc_res == STATUS_OK);
                        rc_res = rc_remove(context.get_node_table().value, parent->ptr_.value_ptr(), parent->ptr_.length);
                        assert(rc_res == STATUS_OK);
                        rc_res = rc_remove(context.get_ptr_table().value, merge_lptr.value_ptr(), merge_lptr.length);
                        assert(rc_res == STATUS_OK);
                        consolidate(parent->lptr_, merge_pptr, rc_version, mergedelta, context);
                        return;
                    }
                    inner_node<Key, Value> newinner(*inner);
                    auto rm_offset = iter - inner->array_.begin();
                    newinner.array_.erase(newinner.array_.begin() + rm_offset);
                    std::vector<uint8_t> data = newinner.serialize();
                    auto rc_res = rc_write(context.get_node_table().value, pptr.value_ptr(), pptr.length, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));
                    assert(rc_res == STATUS_OK);
                    uint64_t rc_version;
                    ramcloud_reject_rules rules;
                    rules.doesntExist = 1;
                    rules.givenVersion = parent->rc_version_;
                    rules.versionNeGiven = 1;
                    if (rm_offset != 0) {
                        rc_res = rc_write_with_reject(context.get_ptr_table().value, parent->lptr_.value_ptr(), parent->lptr_.length, pptr.value_ptr(), pptr.length, &rules, &rc_version);
                    }
                    else {
                        //trigger parent merge
                        remove_delta<Key, Value> rm_delta;
                        rm_delta.next = pptr;
                        rm_delta.low_key = newinner.low_key_;
                        rm_delta.level = newinner.level;
                        auto data = rm_delta.serialize();
                        physical_pointer rm_pptr = context.cache.get_next_physical_ptr();
                        __attribute__((unused)) auto rm_rc_res = rc_write(context.get_node_table().value, rm_pptr.value_ptr(), rm_pptr.length, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));
                        assert(rm_rc_res == STATUS_OK);
                        rc_res = rc_write_with_reject(context.get_ptr_table().value, parent->lptr_.value_ptr(), parent->lptr_.length, rm_pptr.value_ptr(), rm_pptr.length, &rules, &rc_version);
                        if (rc_res != STATUS_OK) {
                            rm_rc_res = rc_remove(context.get_node_table().value, rm_pptr.value_ptr(), rm_pptr.length);
                            assert(rm_rc_res == STATUS_OK);
                        }
                    }
                    if (rc_res == STATUS_WRONG_VERSION || rc_res == STATUS_OBJECT_DOESNT_EXIST) {
                        if (rc_res == STATUS_WRONG_VERSION) {
                            context.cache.invalidate_if_older(parent->lptr_, rc_version);
                        }
                        else {
                            context.cache.invalidate(parent->lptr_);
                        }
                        rc_res = rc_remove(context.get_node_table().value, pptr.value_ptr(), pptr.length);//save
                        assert(rc_res == STATUS_OK);
                        continue;
                    }
                    assert(rc_res == STATUS_OK);
                    rc_res = rc_remove(context.get_node_table().value, parent->ptr_.value_ptr(), parent->ptr_.length);
                    assert(rc_res == STATUS_OK);
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
    static void continue_merge(logical_pointer removedelta_lptr, physical_pointer removedelta_pptr, uint64_t rm_rc_version, remove_delta<Key, Value> *rmdelta, operation_context<Key, Value>& context) {
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
        for (;;) {
            physical_pointer merge_pptr = context.cache.get_next_physical_ptr();
            merge.next = leftp->ptr_;
            data = merge.serialize();
            auto rc_res = rc_write(context.get_node_table().value, merge_pptr.value_ptr(), merge_pptr.length, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));
            assert(rc_res == STATUS_OK);
            ramcloud_reject_rules rules;
            rules.doesntExist = 1;
            rules.givenVersion = leftp->rc_version_;
            rules.versionNeGiven = 1;
            uint64_t merge_rc_version;
            rc_res = rc_write_with_reject(context.get_ptr_table().value, leftp->lptr_.value_ptr(), leftp->lptr_.length, merge_pptr.value_ptr(), merge_pptr.length, &rules, &merge_rc_version);
            if (rc_res == STATUS_WRONG_VERSION || rc_res == STATUS_OBJECT_DOESNT_EXIST) {
                if (rc_res == STATUS_OBJECT_DOESNT_EXIST) {
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
                rc_res = rc_remove(context.get_node_table().value, merge_pptr.value_ptr(), merge_pptr.length);//save
                assert(rc_res == STATUS_OK);
                return;
            }
            assert(rc_res == STATUS_OK);
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
        physical_pointer pptr = context.cache.get_next_physical_ptr();
        std::vector<uint8_t> data = rmdelta.serialize();
        auto rc_res = rc_write(context.get_node_table().value, pptr.value_ptr(), pptr.length, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));
        assert(rc_res == STATUS_OK);
        ramcloud_reject_rules rules;
        rules.doesntExist = 1;
        rules.givenVersion = nodep->rc_version_;
        rules.versionNeGiven = 1;
        uint64_t rc_version;
        rc_res = rc_write_with_reject(context.get_ptr_table().value, nodep->lptr_.value_ptr(), nodep->lptr_.length, pptr.value_ptr(), pptr.length, &rules, &rc_version);
        if (rc_res == STATUS_WRONG_VERSION || rc_res == STATUS_OBJECT_DOESNT_EXIST) {
            if (rc_res == STATUS_WRONG_VERSION) {
                context.cache.invalidate_if_older(nodep->lptr_, rc_version);
            } else {
                context.cache.invalidate(nodep->lptr_);
            }
            rc_res = rc_remove(context.get_node_table().value, pptr.value_ptr(), pptr.length);//save
            assert(rc_res == STATUS_OK);
            return;
        }
        assert(rc_res == STATUS_OK);
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
