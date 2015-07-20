#pragma once
#include "logical_table_cache.h"
#include "base_types.h"

#include <crossbow/allocator.hpp>

#include <atomic>
#include <memory>
#include <mutex>

namespace bdtree {
	template<typename Key, typename Value>
    struct node_pointer : public node<Key, Value> {
        physical_pointer ptr_;
        logical_pointer lptr_;
        std::atomic<uint64_t> last_tx_id_;
        const uint64_t rc_version_;
        mutable node<Key, Value>* node_ = nullptr;
        mutable std::unique_ptr<node_pointer<Key, Value> > old_;
        mutable std::mutex mutex_;
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
                assert(context.locks.find(lptr_) == context.locks.end());
                std::lock_guard<decltype(mutex_)> l(mutex_);
#ifndef NDEBUG
                context.locks.insert(lptr_);
#endif
                if (node_) {
#ifndef NDEBUG
                    context.locks.erase(lptr_);
#endif
                    goto END;
                }

                auto& node_table = context.get_node_table();
                std::error_code ec;
                auto buf = node_table.read(ptr_, ec);
                if (ec == error::object_doesnt_exist) {
#ifndef NDEBUG
                    context.locks.erase(lptr_);
#endif
                    return false;
                }
                assert(!ec);
                auto n = deserialize<Key, Value>(reinterpret_cast<const uint8_t*>(buf.data()), buf.length(), ptr_);
                resolve_operation<Key, Value, Backend> op(lptr_, ptr_, old_.get(), context, rc_version_);
                if (!n->accept(op)) {
#ifndef NDEBUG
                    context.locks.erase(lptr_);
#endif
                    return false;
                }
                crossbow::allocator::destroy(old_.release());
                node_ = op.result;
                op.result = nullptr;
#ifndef NDEBUG
                context.locks.erase(lptr_);
#endif
            }
        END:
            return true;
        }
    };
}
