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
#include <bdtree/bdtree.h>

#include "dummy_backend.hpp"

#include <crossbow/allocator.hpp>

#include <iostream>
#include <thread>
#include <random>

uint8_t* rc_alloc_fun(size_t s) { return new uint8_t[s]; }
void rc_dealloc_fun(uint8_t* b) { delete[] b; }

int main() {
    // Initialize allocator
    crossbow::allocator::init();

    std::unique_ptr<crossbow::allocator> alloc(new crossbow::allocator());

    dummy_backend backend;

    bdtree::logical_table_cache<uint64_t, uint64_t, dummy_backend> cache0;
    bdtree::operation_context<uint64_t, uint64_t, dummy_backend> context0{backend, cache0, bdtree::get_next_tx_id()};
    cache0.get_without_cache(bdtree::logical_pointer{1}, context0);
    bdtree::map<uint64_t, uint64_t, dummy_backend> m0(backend, cache0, bdtree::get_next_tx_id(), true);

    bdtree::logical_table_cache<uint64_t, uint64_t, dummy_backend> cache1;
    bdtree::operation_context<uint64_t, uint64_t, dummy_backend> context1{backend, cache1, 2};
    //cache1.get_without_cache(bdtree::logical_pointer{1}, context1);
    bdtree::map<uint64_t, uint64_t, dummy_backend> m1(backend, cache1, bdtree::get_next_tx_id());

    //bdtree::logical_table_cache<uint64_t, uint64_t> cache3(bdtree::logical_pointer_table{pointer_table_id},bdtree::node_table{node_table_id});
    //bdtree::operation_context<uint64_t, uint64_t> context3{cache1, 3};
    //cache1.get_without_cache(bdtree::logical_pointer{1}, context1);
    bdtree::map<uint64_t, uint64_t, dummy_backend> m3(backend, cache1, bdtree::get_next_tx_id());

    {//insert
        auto inserted = m0.insert(1,1);
        assert(inserted);
    }
    {//find
        auto iter = m0.find(1);
        assert(iter != decltype(iter)());
        auto iter2 = m0.find(2);
        assert(iter2 == decltype(iter2)());
    }
    {//find
        auto iter = m1.find(1);
        assert(iter != decltype(iter)());
        auto iter2 = m1.find(2);
        assert(iter2 == decltype(iter2)());
    }
    {//remove
        auto removed = m1.erase(1);
        assert(removed);
        removed = m0.erase(1);
        assert(!removed);
    }
    alloc.reset(new crossbow::allocator());
    {//insert more
        std::atomic_size_t counter(1);
        std::atomic_size_t inserted_count(0);
        std::vector<std::thread> threads;
        uint thread_num = 20;
        threads.reserve(thread_num);
        for (uint64_t thread_id = 0; thread_id < thread_num; ++thread_id) {
            threads.emplace_back([thread_id, thread_num, &backend, &cache0, &inserted_count](){
                size_t counter = 1;
//                bdtree::logical_table_cache<uint64_t, uint64_t> cache(bdtree::logical_pointer_table{pointer_table_id}, bdtree::node_table{node_table_id});
                bdtree::map<uint64_t, uint64_t, dummy_backend> map(backend, cache0, bdtree::get_next_tx_id());
               do {
                    auto i = counter;
                    auto inserted = map.insert(i,i);
                    if (inserted) {
                        ++inserted_count;
                        //std::cout << "inserted " << i << std::endl;
                    }
                    ++counter;
                }  while (counter <= 50000);
            });
        }
        for (auto& thr: threads) {
            thr.join();
        }
        assert(inserted_count == 50000);

        alloc.reset(new crossbow::allocator());
        bdtree::map<uint64_t, uint64_t, dummy_backend> map(backend, cache0, bdtree::get_next_tx_id());
        auto iter = map.find(1);
        auto end = map.end();
        assert(iter != end);
        uint64_t i = 1;
        for (;iter != end; ++iter) {
            auto key = *iter;
            assert(key.first == i);
            ++i;
        }
    }
    alloc.reset(new crossbow::allocator());
    alloc.reset(new crossbow::allocator());
    alloc.reset(new crossbow::allocator());
    alloc.reset(new crossbow::allocator());
    {
        // test deletion
        std::atomic_size_t deleted_count(0);
        std::atomic_size_t counter(3);
        std::vector<std::thread> threads;
        threads.reserve(2);
        for (uint64_t thread_id = 0; thread_id < 2; ++thread_id) {
            threads.emplace_back([thread_id, &counter, &backend, &deleted_count](){
                crossbow::allocator alloc;
                bdtree::logical_table_cache<uint64_t, uint64_t, dummy_backend> cache;
                bdtree::map<uint64_t, uint64_t, dummy_backend> map(backend, cache, bdtree::get_next_tx_id());
                while (counter <= 10000) {
                    auto i = counter.load();
                    bool deleted = map.erase(i);
                    if (deleted) {
                        ++deleted_count;
                        counter += 3;
                        //std::cout << "deleted " << i << std::endl;
                    }
                }
            });
        }
        for (auto& thr: threads) {
            thr.join();
        }
        alloc.reset(new crossbow::allocator());
        assert(deleted_count == 3333);
        bdtree::logical_table_cache<uint64_t, uint64_t, dummy_backend> cache;
        bdtree::map<uint64_t, uint64_t, dummy_backend> map(backend, cache, bdtree::get_next_tx_id());
        for (uint64_t i = 1; i <= 10000; i++) {
            auto iter = map.find(i);
            bool found = iter->first == i;
            if (found) assert(i % 3);
            else assert(i % 3 == 0);
        }
    }

    alloc.reset(new crossbow::allocator());
    bdtree::logical_table_cache<uint64_t, uint64_t, dummy_backend> cache;
    bdtree::map<uint64_t, uint64_t, dummy_backend> map(backend, cache, bdtree::get_next_tx_id());
    for (uint64_t i = 1; i <= 1; i++) {
        auto iter = map.find(1);
        bool found = iter->first == 1;
        assert(found);
        map.erase(1);
        map.insert(1,1000);
    }
    map.print_statistics();

    return 0;
}
