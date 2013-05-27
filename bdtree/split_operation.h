#pragma once

#include <stack>
#include <mutex>
#include <functional>

#include "primitive_types.h"
#include "deltas.h"
#include "logical_table_cache.h"
#include "util.h"

namespace bdtree {

template<typename Key, typename Value>
struct split_operation {
private:

    template<typename NodeType, typename Func>
    static void consolidate_typed(NodeType* iorl, operation_context<Key, Value>& context, split_delta<Key, Value>* delta, logical_pointer split_lptr, physical_pointer split_pptr, uint64_t rc_version, Func fun) {
        NodeType* consolidated = new NodeType(*iorl);
        physical_pointer ptr = context.cache.get_next_physical_ptr();
        consolidated->array_.erase(consolidated->array_.begin() + consolidated->array_.size()/2, consolidated->array_.end());
        consolidated->right_link_ = delta->new_right;
        consolidated->high_key_ = delta->right_key;
        consolidated->clear_deltas();
        auto data = consolidated->serialize();
        ramcloud_reject_rules rules;
        rules.doesntExist = 1;
        rules.givenVersion = rc_version;
        rules.versionNeGiven = 1;
        auto rc_res = rc_write(context.get_node_table().value, ptr.value_ptr(), ptr.length, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));
        assert(rc_res == STATUS_OK);
        rc_res = rc_write_with_reject(context.get_ptr_table().value, split_lptr.value_ptr(), split_lptr.length, ptr.value_ptr(), ptr.length, &rules, &rc_version);
        if (rc_res == STATUS_OK) {
            node_pointer<Key, Value>* nodep = new node_pointer<Key, Value>(split_lptr, ptr, rc_version);
            consolidated->set_pptr(ptr);
            nodep->node_ = consolidated;
            if (!context.cache.add_entry(nodep, context.tx_id)) {
                delete nodep;
            }
            rc_res = rc_remove(context.get_node_table().value, split_pptr.value_ptr(), split_pptr.length);
            assert(rc_res == STATUS_OK);
            fun(iorl);
            return;
        } else if (rc_res == STATUS_WRONG_VERSION || rc_res == STATUS_OBJECT_DOESNT_EXIST) {
            delete consolidated;
            rc_res = rc_remove(context.get_node_table().value, ptr.value_ptr(), ptr.length);//save
            assert(rc_res == STATUS_OK);
        }
    }

    static void consolidate_split(logical_pointer split_lptr, physical_pointer split_pptr, split_delta<Key, Value>* delta, uint64_t rc_version, operation_context<Key, Value>& context) {
        ramcloud_buffer buf;
        auto rc_res = rc_read(context.get_node_table().value, delta->next.value_ptr(), delta->next.length, &buf);
        if (rc_res == STATUS_OBJECT_DOESNT_EXIST) {
            return;
        }
        assert(rc_res == STATUS_OK);
        auto n = deserialize<Key, Value>(buf.data, buf.length, delta->next);
        resolve_operation<Key, Value> op(split_lptr, delta->next, nullptr, context, rc_version);
        if (!n->accept(op)) {
            return;
        }
        auto nt = op.result->get_node_type();
        if (nt == node_type_t::InnerNode) {
            inner_node<Key, Value>* inner = static_cast<inner_node<Key, Value>*>(op.result);
            consolidate_typed(inner, context, delta, split_lptr, split_pptr, rc_version, [](inner_node<Key, Value>*){});
        } else if (nt == node_type_t::LeafNode) {
            leaf_node<Key, Value>* leaf = static_cast<leaf_node<Key, Value>*>(op.result);
            consolidate_typed(leaf, context, delta, split_lptr, split_pptr, rc_version, [&context, leaf](leaf_node<Key, Value>* n){
                __attribute__((unused)) auto rc_res = rc_remove(context.get_node_table().value, leaf->leaf_pptr_.value_ptr(), leaf->leaf_pptr_.length);
                assert(rc_res == STATUS_OK);
                for (physical_pointer ptr : n->deltas_) {
                    rc_res = rc_remove(context.get_node_table().value, ptr.value_ptr(), ptr.length);
                    assert(rc_res == STATUS_OK);
                }
            });
        } else {
            assert(false);
        }
    }

public:
    static void continue_split(logical_pointer split_lptr, physical_pointer split_pptr, uint64_t split_rc_version, split_delta<Key, Value> *delta, operation_context<Key, Value>& context) {
        assert(context.node_stack.size() >= 2);
        std::function<void()> fun = [](){};
        key_compare<Key, Value> cmp;
        context.node_stack.pop();
        physical_pointer pptr = context.cache.get_next_physical_ptr();
        for (;;) {
            node_pointer<Key, Value> *parent = fix_stack(delta->right_key, context, search_bound::LAST_SMALLER_EQUAL);
            auto ntype = parent->node_->get_node_type();
            if (ntype == node_type_t::LeafNode) {
                fun();
                delete delta;
                return;
            } else if (ntype != node_type_t::InnerNode) {
                assert(false);
            }
            inner_node<Key, Value> *inner = static_cast<inner_node<Key, Value>*>(parent->node_);
            auto iter = last_smaller_equal(inner->array_.begin(), inner->array_.end(), delta->right_key, cmp);
            assert(iter != inner->array_.end());
            if (iter->first == delta->right_key) {
                consolidate_split(split_lptr, split_pptr, delta, split_rc_version, context);
                fun();
                delete delta;
                return;
            }
            if (iter->second != split_lptr) {
                context.node_stack.push(iter->second);
                continue;
            }
            ramcloud_buffer buf;
            auto rc_res = rc_read(context.get_ptr_table().value, split_lptr.value_ptr(), split_lptr.length, &buf);
            if (rc_res == STATUS_OBJECT_DOESNT_EXIST) {
                fun();
                delete delta;
                return;
            }
            assert(rc_res == STATUS_OK);
            if (split_pptr != *reinterpret_cast<physical_pointer*>(buf.data)) {
                fun();
                delete delta;
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
            rc_res = rc_write(context.get_node_table().value, pptr.value_ptr(), pptr.length, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));
            assert(rc_res == STATUS_OK);
            fun = [pptr, &context]() {
                __attribute__((unused)) auto r = rc_remove(context.get_node_table().value, pptr.value_ptr(), pptr.length);
                assert(r == STATUS_OK);
            };
            ramcloud_reject_rules rules;
            rules.doesntExist = 1;
            rules.givenVersion = parent->rc_version_;
            rules.versionNeGiven = 1;
            uint64_t rc_version;
            rc_res = rc_write_with_reject(context.get_ptr_table().value, parent->lptr_.value_ptr(), parent->lptr_.length, pptr.value_ptr(), pptr.length, &rules, &rc_version);
            if (rc_res == STATUS_OK) {
                consolidate_split(split_lptr, split_pptr, delta, split_rc_version, context);
                rc_res = rc_remove(context.get_node_table().value, parent->ptr_.value_ptr(), parent->ptr_.length);
                auto* nnp = new node_pointer<Key, Value>(parent->lptr_, pptr, rc_version);
                nnp->node_ = new_inner;
                if (!context.cache.add_entry(nnp, context.tx_id)) {
                    delete nnp;
                }
                delete delta;
                return;
            } else if (rc_res == STATUS_WRONG_VERSION) {
                delete new_inner;
                continue;
            } else if (rc_res == STATUS_OBJECT_DOESNT_EXIST) {
                delete new_inner;
                context.cache.invalidate(parent->lptr_);
                continue;//TODO: is this correct?
            } else {
                assert(false);
                return;
            }
        }
        assert(false);
    }

    template<typename NodeType>
    static void execute_split(node_pointer<Key, Value>* nodep, NodeType* to_split, operation_context<Key, Value>& context) {
        assert(nodep->node_ == to_split);
        physical_pointer right_pptr = context.cache.get_next_physical_ptr();
        physical_pointer split_ptr = context.cache.get_next_physical_ptr();
        logical_pointer right_lptr = context.cache.get_next_logical_ptr();
        uint64_t lpointer_rc_version;
        auto rc_res = rc_write_with_reject(context.get_ptr_table().value, right_lptr.value_ptr(), right_lptr.length, right_pptr.value_ptr(), right_pptr.length, nullptr, &lpointer_rc_version);
        assert(rc_res == STATUS_OK);
        NodeType* right = new NodeType(right_pptr);
        right->array_.insert(right->array_.begin(), to_split->array_.begin() + to_split->array_.size()/2, to_split->array_.end());
        right->high_key_ = to_split->high_key_;
        right->low_key_ = right->array_.front().first;
        right->right_link_ = to_split->right_link_;
        right->set_level(to_split->level);
        assert(!to_split->high_key_ || *to_split->high_key_ == *right->high_key_);
        std::vector<uint8_t> data = right->serialize();
        rc_res = rc_write(context.get_node_table().value, right_pptr.value_ptr(), right_pptr.length, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));
        assert(rc_res == STATUS_OK);
        if (nodep->lptr_.value == 1) {
            // root split
            assert(to_split->low_key_ == null_key<Key>::value() && !to_split->high_key_);
            logical_pointer lptr_left = context.cache.get_next_logical_ptr();
            physical_pointer pptr_left = context.cache.get_next_physical_ptr();
            physical_pointer pptr_newroot = context.cache.get_next_physical_ptr();
            uint64_t rc_version;
            rc_res = rc_write_with_reject(context.get_ptr_table().value, lptr_left.value_ptr(), lptr_left.length, pptr_left.value_ptr(), pptr_left.length, nullptr, &rc_version);
            assert(rc_res == STATUS_OK);
            node_pointer<Key, Value> *leftp = new node_pointer<Key, Value>(lptr_left, pptr_left, rc_version);
            inner_node<Key, Value> *new_root = new inner_node<Key, Value>(pptr_left);
            new_root->low_key_ = null_key<Key>::value();
            new_root->array_.reserve(2);
            new_root->array_.push_back(std::make_pair(null_key<Key>::value(), lptr_left));
            new_root->array_.push_back(std::make_pair(right->low_key_, right_lptr));
            new_root->set_level(int8_t(to_split->level + 1));
            data = new_root->serialize();
            rc_res = rc_write(context.get_node_table().value, pptr_newroot.value_ptr(), pptr_newroot.length, reinterpret_cast<const char *>(data.data()), uint32_t(data.size()));
            assert(rc_res == STATUS_OK);
            NodeType *left = new NodeType(pptr_left);
            left->array_.insert(left->array_.begin(), to_split->array_.begin(), to_split->array_.begin() + to_split->array_.size()/2);
            left->low_key_ = to_split->low_key_;
            left->high_key_ = right->low_key_;
            left->right_link_ = right_lptr;
            left->set_level(to_split->level);
            leftp->node_ = left;
            data = left->serialize();
            rc_res = rc_write(context.get_node_table().value, pptr_left.value_ptr(), pptr_left.length, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));
            assert(rc_res == STATUS_OK);
            std::lock_guard<std::mutex> _(nodep->mutex_);
            ramcloud_reject_rules rules;
            rules.doesntExist = 1;
            rules.givenVersion = nodep->rc_version_;
            rules.versionNeGiven = 1;
            rc_res = rc_write_with_reject(context.get_ptr_table().value, nodep->lptr_.value_ptr(), nodep->lptr_.length, pptr_newroot.value_ptr(), pptr_newroot.length, &rules, &rc_version);
            assert(rc_res != STATUS_OBJECT_DOESNT_EXIST);//the root node must always exist
            if (rc_res == STATUS_OK) {
                node_pointer<Key, Value>* rightp = new node_pointer<Key, Value>(right_lptr, right_pptr, lpointer_rc_version);
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
            } else if (rc_res == STATUS_WRONG_VERSION) {
                context.cache.invalidate_if_older(nodep->lptr_, rc_version);
                rc_res = rc_remove(context.get_ptr_table().value, right_lptr.value_ptr(), right_lptr.length);//save
                assert(rc_res == STATUS_OK);
                rc_res = rc_remove(context.get_node_table().value, right_pptr.value_ptr(), right_pptr.length);//save
                assert(rc_res == STATUS_OK);
                rc_res = rc_remove(context.get_ptr_table().value, lptr_left.value_ptr(), lptr_left.length);//save
                assert(rc_res == STATUS_OK);
                rc_res = rc_remove(context.get_node_table().value, pptr_newroot.value_ptr(), pptr_newroot.length);//save
                assert(rc_res == STATUS_OK);
                rc_res = rc_remove(context.get_node_table().value, pptr_left.value_ptr(), pptr_left.length);//save
                assert(rc_res == STATUS_OK);
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
        rc_res = rc_write(context.get_node_table().value, split_ptr.value_ptr(), split_ptr.length, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));
        assert(rc_res == STATUS_OK);
        ramcloud_reject_rules rules;
        rules.doesntExist = 1;
        rules.givenVersion = nodep->rc_version_;
        rules.versionNeGiven = 1;
        uint64_t rc_version;
        rc_res = rc_write_with_reject(context.get_ptr_table().value, nodep->lptr_.value_ptr(), nodep->lptr_.length, split_ptr.value_ptr(), split_ptr.length, &rules, &rc_version);
        delete right;
        if (rc_res == STATUS_OK) {
            continue_split(nodep->lptr_, split_ptr, rc_version, split, context);
        } else if (rc_res == STATUS_WRONG_VERSION || rc_res == STATUS_OBJECT_DOESNT_EXIST) {
            if (rc_res == STATUS_WRONG_VERSION) {
                context.cache.invalidate_if_older(nodep->lptr_, rc_version);
            } else {
                context.cache.invalidate(nodep->lptr_);
            }
            rc_res = rc_remove(context.get_ptr_table().value, right_lptr.value_ptr(), right_lptr.length);//save
            assert(rc_res == STATUS_OK);
            rc_res = rc_remove(context.get_node_table().value, right_pptr.value_ptr(), right_pptr.length);//save
            assert(rc_res == STATUS_OK);
            rc_res = rc_remove(context.get_node_table().value, split_ptr.value_ptr(), split_ptr.length);//save
            assert(rc_res == STATUS_OK);
            delete split;
        } else {
            assert(false);
        }
    }

    static void split(node_pointer<Key, Value>* node, operation_context<Key, Value>& context) {
        node_type_t nt = node->node_->get_node_type();
        if (nt == node_type_t::LeafNode) {
            execute_split(node, node->as_leaf(), context);
        } else if (nt == node_type_t::InnerNode) {
            execute_split(node, node->as_inner(), context);
        }
    }
};

}
