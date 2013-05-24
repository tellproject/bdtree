#pragma once

#include <boost/optional.hpp>
#include <boost/none.hpp>
#include <algorithm>

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
        
        erase_result erase_if_no_newer() {
            assert(!after());
            leaf_node<Key, Value>* leaf = current_->as_leaf();
            bool consolidate = leaf->deltas_.size() >= CONSOLIDATE_AT;
            leaf_node<Key, Value>* nl = new leaf_node<Key, Value>(*leaf);
            auto iter = nl->array_.find(current_iterator_->first);
            nl->array_.erase(iter);
            physical_pointer pptr = context_->cache.get_next_physical_ptr();
            std::vector<uint8_t> data;
            auto current_index = current_iterator_ - current_->array_.begin();
            if (consolidate) {
                data = nl->serialize();
            } else {
                delete_delta<Key, Value> dd;
                dd.key = current_iterator_->first;
                dd.next = current_->ptr_;
                data = dd.serialize();
                nl->deltas_.push_back(pptr);
            }
            auto s = nl->serialized_size();
            if (s < MIN_NODE_SIZE) {
                merge_operation<Key, Value>::execute_merge(current_, leaf, *context_);
                return erase_result::Merged;
            }
            auto rc_res = rc_write(context_->get_node_table().value, pptr.value_ptr(), pptr.length, reinterpret_cast<char*>(data.data()), data.size());
            assert(rc_res == STATUS_OK);
            ramcloud_reject_rules rules;
            rules.doesntExist = 1;
            rules.givenVersion = current_->rc_version;
            rules.versionNeGiven = 1;
            uint64_t rc_version;
            rc_res = rc_write_with_reject(context_->get_ptr_table().value, current_->lptr_.value_ptr(), current_->lptr_.length, pptr.value_ptr(), pptr.length, &rules, &rc_version);
            if (rc_res == STATUS_WRONG_VERSION || rc_res == STATUS_OBJECT_DOESNT_EXIST) {
                delete nl;
                rc_res = rc_remove(context_->get_node_table().value, pptr.value_ptr(), pptr.length);//safe
                return erase_result::Failed;
            }
            node_pointer<Key, Value>* np = new node_pointer<Key, Value>(current_->lptr_, pptr, rc_version);
            np->node_ = nl;
            if (!context_->cache.add_entry(np)) {
                delete np;
                ++(*this);
            } else {
                current_ = np;
                if (current_->array_.size() <= current_index) {
                    current_iterator_ = current_->array_.end() - 1;
                    ++(*this);
                } else {
                    current_iterator_ = current_->array_.begin() + current_index;
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
            auto old_current = current_;
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
            return (**this).first == (*other).first;//TODO: nicer
        }
        bool operator !=(const bdtree_iterator<Key,Value>& other) const {
            return !(*this == other);
        }
        const std::pair<Key, Value>& operator *() const {
            return *current_iterator_;
        }
        const std::pair<Key, Value>* operator ->() const {
            return &(*current_iterator_);
        }
    };
}
