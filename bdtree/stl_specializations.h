#pragma once
#include "forward_declarations.h"

namespace std {
    template<typename Key, typename Value>
    struct less<std::pair<Key, bdtree::node<Key, Value>*>>
    {
        typedef std::pair<Key, bdtree::node<Key, Value>*> operand;
        std::less<Key> less;
        bool operator() (const operand& lhs, const operand& rhs) const
        {
            return less(lhs.first, rhs.first);
        }
    };
    
    template<>
    struct hash<bdtree::logical_pointer> {
        hash<uint64_t> hasher_;
        size_t operator() (bdtree::logical_pointer op) const {
            return hasher_(op.value);
        }
    };
    
    template<>
    struct hash<bdtree::physical_pointer> {
        hash<uint64_t> hasher_;
        size_t operator() (bdtree::physical_pointer op) const {
            return hasher_(op.value);
        }
    };
    
    template<>
    struct equal_to<bdtree::logical_pointer> {
        equal_to<uint64_t> eq;
        bool operator() (bdtree::logical_pointer lhs, bdtree::logical_pointer rhs) const {
            return eq(lhs.value, rhs.value);
        }
    };
    
    template<>
    struct equal_to<bdtree::physical_pointer> {
        equal_to<uint64_t> eq;
        bool operator() (bdtree::physical_pointer lhs, bdtree::physical_pointer rhs) const {
            return eq(lhs.value, rhs.value);
        }
    };
}
