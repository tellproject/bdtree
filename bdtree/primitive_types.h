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
    extern void got_tx_id(uint64_t tx_id);
    extern uint64_t get_last_tx_id();
    
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
