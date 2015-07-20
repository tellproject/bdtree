#pragma once

#include <bdtree/bdtree.h>
#include <bdtree/config.h>
#include <bdtree/primitive_types.h>
#include <bdtree/base_types.h>
#include <bdtree/util.h>
#include <bdtree/merge_operation.h>

#include <boost/optional.hpp>
#include <boost/none.hpp>

#include <algorithm>

namespace bdtree {
    template<typename Key, typename Value, typename Backend>
    struct bdtree_iterator : public std::iterator<std::bidirectional_iterator_tag, const std::pair<Key, Value>> {
        typedef std::bidirectional_iterator_tag Category;
        boost::optional<operation_context<Key, Value, Backend>> context_;
        node_pointer<Key, Value>* current_ = nullptr;
        typename decltype(leaf_node<Key,Value>::array_)::iterator current_iterator_;
        key_compare<Key, Value> cmp_;
        bdtree_iterator(){}
        bdtree_iterator(operation_context<Key, Value, Backend> && context, decltype(current_) n, const Key & key)
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
            if (s < MIN_NODE_SIZE && current_->lptr_.value != 1) {
                merge_operation<Key, Value, Backend>::execute_merge(current_, leaf, *context_);
                return erase_result::Merged;
            }
            data = nl->serialize();

            auto& node_table = context_->get_node_table();
            auto pptr = node_table.get_next_ptr();
            nl->leaf_pptr_ = pptr;
            auto last_tx_id = get_last_tx_id();

            node_table.insert(pptr, reinterpret_cast<char*>(data.data()), data.size());

            auto& ptr_table = context_->get_ptr_table();
            std::error_code ec;
            auto lptr_version = ptr_table.update(current_->lptr_, pptr, current_->rc_version_, ec);
            if (ec == error::wrong_version || ec == error::object_doesnt_exist) {
                delete nl;
                node_table.remove(pptr);
                return erase_result::Failed;
            }

            node_pointer<Key, Value>* np = new node_pointer<Key, Value>(current_->lptr_, pptr, lptr_version);
            np->node_ = nl;
            static_assert(CONSOLIDATE_AT == 0 && FakeParam == FakeParam, "bdtree_iterator::erase_if_no_newer cannot correctly handle delta chains");
            node_table.remove(current_->ptr_);
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

        bdtree_iterator<Key, Value, Backend>& operator ++() {
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


        bdtree_iterator<Key, Value, Backend>& operator --() {
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
        bool operator ==(const bdtree_iterator<Key, Value, Backend>& other) const {
            if (other.after() || after()) {
                return current_ == other.current_;
            }
            return (**this) == (*other);//TODO: nicer
        }
        bool operator !=(const bdtree_iterator<Key, Value, Backend>& other) const {
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
