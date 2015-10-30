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
#include "logical_table_cache.h"
#include "base_types.h"
#include "error_code.h"

#include <crossbow/allocator.hpp>

#include <atomic>
#include <memory>

namespace bdtree {
	template<typename Key, typename Value>
    struct node_pointer : public node<Key, Value> {
        physical_pointer ptr_;
        logical_pointer lptr_;
        std::atomic<uint64_t> last_tx_id_;
        const uint64_t rc_version_;
        mutable node<Key, Value>* node_ = nullptr;
        mutable std::unique_ptr<node_pointer<Key, Value> > old_;
    public: // Construction/Destruction
        node_pointer(logical_pointer lptr, physical_pointer pointer, uint64_t rc_version)
        : ptr_(pointer), lptr_(lptr), last_tx_id_(0), rc_version_(rc_version) {}
        virtual ~node_pointer() {
            delete node_;
        }
    public: // operations
        bool accept(operation<Key, Value>& op) override {
            return op.visit(*this);
        }
        std::vector<uint8_t> serialize() const override {
            assert(false);
            return std::vector<uint8_t>();
        }

        bool is_root() const {
            return lptr_.value == 1;
        }
        
        node_type_t get_node_type() const override {
            return node_type_t::NodePointer;
        }

        leaf_node<Key,Value>* as_leaf() const {
            assert(node_);
            assert(node_->get_node_type() == node_type_t::LeafNode);
            return static_cast<leaf_node<Key,Value>*>(node_);
        }

        inner_node<Key, Value>* as_inner() const {
            assert(node_);
            assert(node_->get_node_type() == node_type_t::InnerNode);
            return static_cast<inner_node<Key,Value>*>(node_);
        }
        
        void reset_old(node_pointer<Key, Value> *o) {
            crossbow::allocator::destroy(old_.release());
            old_.reset(o);
        }
        
        void release_old() {
            old_.release();
        }
        
        template <typename Backend>
        bool resolve(operation_context<Key, Value, Backend>& context) {
            if (!node_) {
                if (node_) {
                    goto END;
                }

                auto& node_table = context.get_node_table();
                std::error_code ec;
                auto buf = node_table.read(ptr_, ec);
                if (ec == error::object_doesnt_exist) {
                    return false;
                }
                assert(!ec);
                auto n = deserialize<Key, Value>(reinterpret_cast<const uint8_t*>(buf.data()), buf.length(), ptr_);
                resolve_operation<Key, Value, Backend> op(lptr_, ptr_, old_.get(), context, rc_version_);
                if (!n->accept(op)) {
                    return false;
                }
                crossbow::allocator::destroy(old_.release());
                node_ = op.result;
                op.result = nullptr;
            }
        END:
            return true;
        }
    };
}
