#pragma once

#include <bdtree/config.h>

#include "logical_table_cache.h"
#include "search_operation.h"
#include <cramcloud.h>
#include "split_operation.h"
#include "merge_operation.h"

namespace bdtree {

template<typename Key, typename Value>
struct leaf_operation_base {
    bool consolidated = false;

    void cleanup(node_table tableid, const std::vector<physical_pointer>& ptrs) {
        if (!consolidated) return;
        for (physical_pointer ptr : ptrs) {
//            std::cout << "cleaning " << ptr.value << std::endl;
            __attribute__((unused)) auto rc_res = rc_remove(tableid.value, ptr.value_ptr(), ptr.length);
            assert(rc_res == STATUS_OK);
        }
    }
};

template<typename Key, typename Value, typename Compare = key_compare<Key, Value> >
struct insert_operation : public leaf_operation_base<Key, Value> {
    const Key& key;
    const Value& value;
    Compare comp;

    insert_operation(const Key& key, const Value& value, Compare comp)
        : key(key), value(value), comp(comp)
    {}

    bool has_conflicts(leaf_node<Key, Value>* leafp) {
        return std::binary_search(leafp->array_.begin(), leafp->array_.end(), key, comp);
    }

    std::vector<uint8_t> operator() (const node_pointer<Key, Value>* nptr, leaf_node<Key, Value>& ln, physical_pointer pptr) {
        auto iter = std::lower_bound(ln.array_.begin(), ln.array_.end(), key, comp);
        ln.array_.insert(iter, std::make_pair(key, value));
        auto leafp = nptr->as_leaf();
        auto deltasize = leafp->deltas_.size();
        if (deltasize + 1 >= CONSOLIDATE_AT) {
            ln.deltas_.clear();
            ln.leaf_pptr_ = pptr;
            this->consolidated = true;
            return ln.serialize();
        } else {
            this->consolidated = false;
            insert_delta<Key, Value> ins_delta;
            ins_delta.value = std::make_pair(key, value);
            ins_delta.next = nptr->ptr_;
            ln.deltas_.insert(ln.deltas_.begin(), pptr);
            return ins_delta.serialize();
        }
    }
};

template<typename Key, typename Value, typename Compare = key_compare<Key, Value> >
struct delete_operation : public leaf_operation_base<Key, Value> {
    const Key& key;
    Compare comp;

    delete_operation(const Key& key, Compare comp)
        : key(key), comp(comp)
    {}

    bool has_conflicts(leaf_node<Key, Value>* leafp) {
        return !std::binary_search(leafp->array_.begin(), leafp->array_.end(), key, comp);
    }

    std::vector<uint8_t> operator() (const node_pointer<Key, Value>* nptr, leaf_node<Key, Value>& ln, physical_pointer pptr) {
        auto iter = std::lower_bound(ln.array_.begin(), ln.array_.end(), key, comp);
        assert(iter->first == key);
        ln.array_.erase(iter);
        auto leafp = nptr->as_leaf();
        if (leafp->deltas_.size() + 1 >= CONSOLIDATE_AT) {
            this->consolidated = true;
            ln.deltas_.clear();
            ln.leaf_pptr_ = pptr;
            return ln.serialize();
        } else {
            this->consolidated = false;
            delete_delta<Key, Value> del_delta;
            del_delta.key = key;
            del_delta.next = nptr->ptr_;
            ln.deltas_.insert(ln.deltas_.begin(), pptr);
            return del_delta.serialize();
        }
    }
};

template<typename Key, typename Value, typename Operation>
bool exec_leaf_operation(const Key& key, logical_table_cache<Key, Value>& cache, uint64_t tx_id, Operation op) {
    physical_pointer pptr;
    // find the insert/erase candidate
    auto leaf = lower_node_bound(key, cache, tx_id);
    std::size_t nsize = leaf.first->as_leaf()->serialized_size();
    if (nsize >= MAX_NODE_SIZE) {
        split_operation<Key, Value>::split(leaf.first, leaf.second);
        return exec_leaf_operation(key, cache, tx_id, op);
    } else if (nsize < MIN_NODE_SIZE
               && !(leaf.first->as_leaf()->low_key_ == null_key<Key>::value() && !leaf.first->as_leaf()->high_key_)) {
        merge_operation<Key, Value>::merge(leaf.first, leaf.second);
        return exec_leaf_operation(key, cache, tx_id, op);
    }
    auto context = std::move(leaf.second);
    // create new leaf node for cache (and may be for consolidation)
    std::unique_ptr<leaf_node<Key, Value>> lnptr;
    for (;;) {
        leaf_node<Key, Value>* leafp = leaf.first->as_leaf();
        if (op.has_conflicts(leafp)) {
            if (lnptr) {
                __attribute__((unused)) auto res = rc_remove(cache.get_node_table().value, pptr.value_ptr(), pptr.length);
                assert(res == STATUS_OK);
            }
            return false;
        }
        if (!lnptr) {
            pptr = cache.get_next_physical_ptr();
            lnptr.reset(new leaf_node<Key, Value>(pptr));
        }

        leaf_node<Key, Value>& ln = *lnptr;
        ln = *leafp;
        // create and get the serialized delta node or consolidated node
        std::vector<uint8_t> data = op(leaf.first, ln, pptr);
        auto rc_res = ::rc_write(cache.get_node_table().value, pptr.value_ptr(), pptr.length, reinterpret_cast<const char*>(data.data()), uint32_t(data.size()));
        assert(rc_res == STATUS_OK);
        ramcloud_reject_rules rules;
        rules.doesntExist = 1;
        rules.givenVersion = leaf.first->rc_version_;
        rules.versionNeGiven = 1;
        uint64_t new_version;
        // do the compare and swap
        rc_res = rc_write_with_reject(cache.get_ptr_table().value, leaf.first->lptr_.value_ptr(), logical_pointer::length, pptr.value_ptr(), physical_pointer::length, &rules, &new_version);
        if (rc_res == STATUS_OK) {
            node_pointer<Key, Value>* nnp = new node_pointer<Key, Value>(leaf.first->lptr_, pptr, new_version);
            nnp->node_ = lnptr.release();
            if (!cache.add_entry(nnp, tx_id)) {
                delete nnp;
            }
            std::vector<physical_pointer> ptrs = leafp->deltas_;
            ptrs.push_back(leafp->leaf_pptr_);
            // do the cleanup if consolidated
            op.cleanup(cache.get_node_table(), ptrs);
            return true;
        } else if (rc_res == STATUS_OBJECT_DOESNT_EXIST) {
            context.cache.invalidate(leaf.first->lptr_);
        } else if (rc_res != STATUS_WRONG_VERSION) {
            assert(false);//TODO: handle STATUS_OBJECT_DOESNT_EXIST
        }
        leaf.first = lower_bound_node_with_context(key, context, search_bound::LAST_SMALLER_EQUAL, cache_use::None);
    }
    assert(false);
    return true;
}

}
