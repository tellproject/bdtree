#pragma once
#include <cassert>
#include <algorithm>
#include <stack>

#include <bdtree/config.h>
#include "forward_declarations.h"
#include "node_pointer.h"
#include "deltas.h"
#include "split_operation.h"
#include "merge_operation.h"

namespace bdtree {
    template<typename Key, typename Value, typename Backend>
    struct resolve_operation : public operation<Key, Value> {
        logical_pointer lptr_;
        node_pointer<Key, Value>* old;
        operation_context<Key, Value, Backend>& context_;
        resolve_operation(logical_pointer lptr, physical_pointer lastpptr, decltype(old) o, operation_context<Key, Value, Backend>& context, uint64_t rc_version)
            : lptr_(lptr), old(o), context_(context), lastpptr(lastpptr), rc_version(rc_version) {}
        virtual ~resolve_operation() {
            for (auto p : deltas) {
                delete p.second;
            }
            delete result;
        }

        node<Key, Value>* result = nullptr;
        std::vector<std::pair<physical_pointer, node<Key, Value>*>> deltas;
        physical_pointer lastpptr;
        uint64_t rc_version;
        bool visit(node_pointer<Key, Value>& node) override {
            assert(false);
            return true;
        }
        bool visit(inner_node<Key, Value>& n) override {
            assert(deltas.size() == 0);
            result = &n;
            return true;
        }

        bool visit(leaf_node<Key, Value>& n) override {
            key_compare<Key, Value> cmp;
            std::vector<physical_pointer> old_deltas = std::move(n.deltas_);
            for (auto iter = deltas.rbegin(); iter != deltas.rend(); ++iter) {
                n.deltas_.push_back(iter->first);
                node<Key, Value>* i = iter->second;
                switch (i->get_node_type()) {
                case node_type_t::InsertDelta:
                {
                    insert_delta<Key, Value>* d = static_cast<insert_delta<Key, Value>*>(i);
                    auto ins_pos = std::lower_bound(n.array_.begin(), n.array_.end(), d->value.first, cmp);
                    assert(ins_pos == n.array_.end() || ins_pos->first != d->value.first);
                    n.array_.insert(ins_pos, d->value);
                }
                    break;
                case node_type_t::DeleteDelta:
                {
                    delete_delta<Key, Value>* d = static_cast<delete_delta<Key, Value>*>(i);
                    auto del_pos = std::lower_bound(n.array_.begin(), n.array_.end(), d->key, cmp);
                    assert(del_pos != n.array_.end() && del_pos->first == d->key);
                    n.array_.erase(del_pos);
                }
                    break;
                default:
                    assert(false);
                    return false;
                }
            }
            n.deltas_.reserve(n.deltas_.size() + old_deltas.size());
            n.deltas_.insert(n.deltas_.end(), old_deltas.begin(), old_deltas.end());
            result = &n;
            return true;
        }

        bool visit(insert_delta<Key, Value>& node)  override {
            return visit_delta(node);
        }

        bool visit(delete_delta<Key, Value>& node) override {
            return visit_delta(node);
        }

        bool visit(split_delta<Key, Value>& node) override {
            auto old_stack = context_.node_stack;
            split_operation<Key, Value, Backend>::continue_split(lptr_, lastpptr, rc_version, &node, context_);
            context_.node_stack = old_stack;
            delete &node;
            return false;
        }

        bool visit(remove_delta<Key, Value>& node) override {
            auto old_stack = context_.node_stack;
            merge_operation<Key, Value, Backend>::continue_merge(lptr_, lastpptr, rc_version, &node, context_);
            context_.node_stack = old_stack;
            delete &node;
            return false;
        }

        bool visit(merge_delta<Key, Value>& node) override {
            auto old_stack = context_.node_stack;
            merge_operation<Key, Value, Backend>::continue_merge(lptr_, lastpptr, rc_version, &node, context_);
            context_.node_stack = old_stack;
            delete &node;
            return false;
        }
        
        template<typename Node>
        bool visit_delta(Node& node) {
            deltas.push_back(std::make_pair(lastpptr, &node));
            if (old && node.next == old->ptr_ && old->resolve(context_)) {
                auto *res = old->node_->copy();
                old = nullptr;
                return res->accept(*this);
            }
            auto& node_table = context_.get_node_table();
            std::error_code ec;
            auto buf = node_table.read(node.next, ec);
            lastpptr = node.next;
            if (ec) {
                return false;
            }
            auto res = deserialize<Key, Value>(reinterpret_cast<const uint8_t*>(buf.data()), buf.length(), node.next);
            return res->accept(*this);
        }
    };
}
