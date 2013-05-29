#pragma once

#include <boost/optional.hpp>
#include <boost/none.hpp>
#include <algorithm>

#include <bdtree.h>
#include "primitive_types.h"
#include "base_types.h"
#include "util.h"
#include "merge_operation.h"
#include <bdtree/config.h>

namespace bdtree {
    template<typename Key, typename Value>
    struct bdtree_iterator : public std::iterator<std::bidirectional_iterator_tag, const std::pair<Key, Value>> {
        typedef std::bidirectional_iterator_tag Category;
        boost::optional<operation_context<Key, Value>> context_;
        node_pointer<Key, Value>* current_ = nullptr;
        typename decltype(leaf_node<Key,Value>::array_)::iterator current_iterator_;
        key_compare<Key, Value> cmp_;
        bdtree_iterator(){}
        bdtree_iterator(operation_context<Key, Value> && context, decltype(current_) n, const Key & key)
            : context_(std::move(context)), current_(n) {
            assert(current_ != nullptr);
            current_iterator_ = std::lower_bound(current_->as_leaf()->array_.begin(), current_->as_leaf()->array_.end(), key, cmp_);
            while (current_iterator_ == current_->as_leaf()->array_.end()) {
                if (set_void_if_after())
                    return;
                current_ = get_next(*context_, current_);
                current_iterator_ = std::lower_bound(current_->as_leaf()->array_.begin(), current_->as_leaf()->array_.end(), key, cmp_);
            }
        }
        
        template <uint FakeParam = 1>
        erase_result erase_if_no_newer() {
            assert(!after());
            leaf_node<Key, Value>* leaf = current_->as_leaf();
            leaf_node<Key, Value>* nl = new leaf_node<Key, Value>(*leaf);
            size_t current_index = current_iterator_ - current_->as_leaf()->array_.begin();
            nl->array_.erase(nl->array_.begin() + current_index);
            std::vector<uint8_t> data;
            static_assert(CONSOLIDATE_AT == 0 && FakeParam == 1, "bdtree_iterator::erase_if_no_newer cannot correctly handle delta chains");
            assert(nl->deltas_.size() == 0);
            auto s = nl->serialized_size();
            if (s < MIN_NODE_SIZE) {
                merge_operation<Key, Value>::execute_merge(current_, leaf, *context_);
                return erase_result::Merged;
            }
            data = nl->serialize();
            physical_pointer pptr = context_->cache.get_next_physical_ptr();
            nl->leaf_pptr_ = pptr;
            auto rc_res = rc_write(context_->get_node_table().value, pptr.value_ptr(), pptr.length, reinterpret_cast<char*>(data.data()), data.size());
            assert(rc_res == STATUS_OK);
            ramcloud_reject_rules rules;
            rules.doesntExist = 1;
            rules.givenVersion = current_->rc_version_;
            rules.versionNeGiven = 1;
            uint64_t rc_version;
            auto last_tx_id = get_last_tx_id();
            rc_res = rc_write_with_reject(context_->get_ptr_table().value, current_->lptr_.value_ptr(), current_->lptr_.length, pptr.value_ptr(), pptr.length, &rules, &rc_version);
            if (rc_res == STATUS_WRONG_VERSION || rc_res == STATUS_OBJECT_DOESNT_EXIST) {
                delete nl;
                rc_res = rc_remove(context_->get_node_table().value, pptr.value_ptr(), pptr.length);
                return erase_result::Failed;
            }
            node_pointer<Key, Value>* np = new node_pointer<Key, Value>(current_->lptr_, pptr, rc_version);
            np->node_ = nl;
            static_assert(CONSOLIDATE_AT == 0 && FakeParam == FakeParam, "bdtree_iterator::erase_if_no_newer cannot correctly handle delta chains");
            rc_res = rc_remove(context_->get_node_table().value, current_->ptr_.value_ptr(), current_->ptr_.length);
            assert(rc_res == STATUS_OK);
            if (!context_->cache.add_entry(np, last_tx_id)) {
                delete np;
                ++(*this);
            } else {
                current_ = np;
                if (current_->as_leaf()->array_.size() <= current_index) {
                    current_iterator_ = current_->as_leaf()->array_.end() - 1;
                    ++(*this);
                } else {
                    current_iterator_ = current_->as_leaf()->array_.begin() + current_index;
                }
            }
            return erase_result::Success;
        }

        void set_void() {
            current_ = nullptr;
            context_ = boost::none;
        }

        bool set_void_if_after() {
            if (!current_->as_leaf()->high_key_) {
                set_void();
                return true;
            }
            return false;
        }

        bool set_void_if_before() {
            if (current_->as_leaf()->low_key_ == null_key<Key>::value()) {
                set_void();
                return true;
            }
            return false;
        }

        bdtree_iterator<Key, Value>& operator ++() {
            assert(!after());
            //leaf_node<Key, Value>* np = get_next(*context_, current_);
            ++current_iterator_;
            if (current_iterator_ != current_->as_leaf()->array_.end()) {
                return *this;
            }
            if (set_void_if_after()) {
                return *this;
            }
            auto hkey = *current_->as_leaf()->high_key_;
#ifndef NDEBUG
            auto old_current = current_;
#endif
            do {
                if (set_void_if_after()) {
                    return *this;
                }
                current_ = get_next(*context_, current_);
            } while (false);
            assert(!current_->as_leaf()->array_.empty());
            if (current_->as_leaf()->low_key_ == hkey) {
                current_iterator_ = current_->as_leaf()->array_.begin();
                return *this;
            }
            assert(old_current->as_leaf()->right_link_ != current_->lptr_ || current_->as_leaf()->low_key_ == hkey);
            for (;;) {
                current_iterator_ = std::lower_bound(current_->as_leaf()->array_.begin(), current_->as_leaf()->array_.end(), hkey, cmp_);
                if (current_iterator_ == current_->as_leaf()->array_.end()) {
                    if (set_void_if_after()) {
                        return *this;
                    }
                    current_ = get_next(*context_, current_);
                } else {
                    return *this;
                }
            }
            assert(false);
            return *this;
        }


        bdtree_iterator<Key, Value>& operator --() {
            assert(!after());
            --current_iterator_;
            if (current_iterator_ != current_->as_leaf()->array_.begin() - 1) {
                return *this;
            }
            do {
                if (set_void_if_before()) {
                    return *this;
                }
                current_ = get_previous(*context_, current_);
            } while (current_->as_leaf()->array_.empty());
            auto& lkey = current_->as_leaf()->low_key_;
            if (current_->as_leaf()->high_key_ && *current_->as_leaf()->high_key_ == lkey) {
                current_iterator_ = current_->as_leaf()->array_.end() - 1;
                return *this;
            }
            for (;;) {
                current_iterator_ = last_smaller(current_->as_leaf()->array_.begin(), current_->as_leaf()->array_.end(), lkey, cmp_);
                if (current_iterator_ == current_->as_leaf()->array_.end()) {
                    if (set_void_if_before()) return *this;
                    current_ = get_previous(*context_, current_);
                } else {
                    return *this;
                }
            }
            assert(false);
            return *this;
        }

        bool after() const {
            assert((current_ == nullptr && !context_) || (current_ && context_));
            return current_ == nullptr || !context_;
        }
        bool operator ==(const bdtree_iterator<Key,Value>& other) const {
            if (other.after() || after()) {
                return current_ == other.current_;
            }
            return (**this) == (*other);//TODO: nicer
        }
        bool operator !=(const bdtree_iterator<Key,Value>& other) const {
            return !(*this == other);
        }
        template <typename V = Value>
        typename std::enable_if<std::is_same<V, bdtree::empty_t>::value, const Key&>::type operator *() const {
            return current_iterator_->first;
        }

        template <typename V = Value>
        typename std::enable_if<!std::is_same<V, bdtree::empty_t>::value, decltype(*current_iterator_)>::type operator *() const {
            return *current_iterator_;
        }
        template <typename V = Value>
        typename std::enable_if<std::is_same<V, bdtree::empty_t>::value, const Key*>::type operator ->() const {
            return &(**this);
        }
        template <typename V = Value>
        typename std::enable_if<!std::is_same<V, bdtree::empty_t>::value, decltype(&*current_iterator_)>::type operator ->() const {
            return &(**this);
        }
    };
}
