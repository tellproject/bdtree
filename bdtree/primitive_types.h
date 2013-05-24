#pragma once
#include <stdint.h>
#include <functional>

namespace bdtree {
    
    template<typename Key, typename Value, typename Compare = std::less<Key> >
    struct key_compare {
        Compare compare_;
        key_compare() = default;
        key_compare(Compare compare) : compare_(compare) {}
        
        template<typename T>
        bool operator() (const Key k, const std::pair<Key, T>& p) {
            return compare_(k, p.first);
        }
        
        template<typename T>
        bool operator() (const std::pair<Key, T>& p, const Key& k) {
            return compare_(p.first, k);
        }
    };
    
    extern uint64_t get_next_tx_id();
    extern uint64_t get_last_tx_id();
    
    struct logical_pointer_table {
        uint64_t value;
        bool operator== (logical_pointer_table other) const {
            return value == other.value;
        }
    };
    
    struct node_table {
        uint64_t value;
        bool operator== (node_table other) const {
            return value == other.value;
        }
    };
    
    struct logical_pointer {
        uint64_t value;
        static constexpr uint16_t length = sizeof(uint64_t);
        const char* value_ptr() const {
            return reinterpret_cast<const char*>(&value);
        }
        bool operator== (logical_pointer other) const {
            return value == other.value;
        }
        bool operator!= (logical_pointer other) const {
            return !(*this == other);
        }
    };
    
    struct physical_pointer {
        uint64_t value;
        static constexpr uint16_t length = sizeof(uint64_t);
        const char* value_ptr() const {
            return reinterpret_cast<const char*>(&value);
        }

        bool operator== (physical_pointer other) const {
            return value == other.value;
        }
        bool operator!= (physical_pointer other) const {
            return !(*this == other);
        }
    };
    
    enum class node_type_t : uint8_t {
        NodePointer = 0,
        InnerNode,
        LeafNode,
        InsertDelta,
        DeleteDelta,
        SplitDelta,
        RemoveDelta,
        MergeDelta
    };

}
