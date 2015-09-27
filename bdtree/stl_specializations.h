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
