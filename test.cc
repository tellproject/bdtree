#include <iostream>
#include <thread>
#include <random>

#include "bdtree.h"

uint8_t* rc_alloc_fun(size_t s) { return new uint8_t[s]; }
void rc_dealloc_fun(uint8_t* b) { delete[] b; }

int main() {
    awesome::init();
    {
    std::unique_ptr<awesome::allocator> alloc(new awesome::allocator());
    init_ram_cloud("", &rc_alloc_fun, &rc_dealloc_fun);
    auto err = rc_create_table("pointer_table");
    assert(err == STATUS_OK);
    err = rc_create_table("counter");
    assert(err == STATUS_OK);
    uint64_t pointer_table_id;
    err = rc_get_table_id(&pointer_table_id, "pointer_table");
    assert(err == STATUS_OK);
    err = rc_create_table("node_table");
    assert(err == STATUS_OK);
    uint64_t node_table_id;
    err = rc_get_table_id(&node_table_id, "node_table");
    assert(err == STATUS_OK);
    bdtree::logical_table_cache<uint64_t, uint64_t> cache0(bdtree::logical_pointer_table{pointer_table_id}, bdtree::node_table{node_table_id});
    bdtree::operation_context<uint64_t, uint64_t> context0{cache0, bdtree::get_next_tx_id()};
    cache0.get_without_cache(bdtree::logical_pointer{1}, context0);
    bdtree::map<uint64_t, uint64_t> m0(cache0, bdtree::get_next_tx_id(), true);

    bdtree::logical_table_cache<uint64_t, uint64_t> cache1(bdtree::logical_pointer_table{pointer_table_id}, bdtree::node_table{node_table_id});
    bdtree::operation_context<uint64_t, uint64_t> context1{cache1, 2};
    //cache1.get_without_cache(bdtree::logical_pointer{1}, context1);
    bdtree::map<uint64_t, uint64_t> m1(cache1, bdtree::get_next_tx_id());

    //bdtree::logical_table_cache<uint64_t, uint64_t> cache3(bdtree::logical_pointer_table{pointer_table_id},bdtree::node_table{node_table_id});
    //bdtree::operation_context<uint64_t, uint64_t> context3{cache1, 3};
    //cache1.get_without_cache(bdtree::logical_pointer{1}, context1);
    bdtree::map<uint64_t, uint64_t> m3(cache1, bdtree::get_next_tx_id());

    {//insert
        auto inserted = m0.insert(1,1);
        assert(inserted);
//        inserted = m0.insert(1,1);
//        assert(!inserted);
    }
    {//find
        auto iter = m0.find(1);
        assert(iter != decltype(iter)());
        //    iter = m.find(2);
        auto iter2 = m0.find(2);
        assert(iter2 == decltype(iter2)());
    }
    {//find
        auto iter = m1.find(1);
        assert(iter != decltype(iter)());
        //    iter = m.find(2);
        auto iter2 = m1.find(2);
        assert(iter2 == decltype(iter2)());
    }
    {//remove
        auto removed = m1.erase(1);
        assert(removed);
        removed = m0.erase(1);
        assert(!removed);
    }
    alloc.reset(new awesome::allocator());
    {//insert more
        std::atomic_size_t counter(1);
        std::atomic_size_t inserted_count(0);
        std::vector<std::thread> threads;
        uint thread_num = 20;
        threads.reserve(thread_num);
        for (uint64_t thread_id = 0; thread_id < thread_num; ++thread_id) {
            threads.emplace_back([thread_id, thread_num, pointer_table_id, node_table_id, &cache0, &inserted_count](){
                size_t counter = 1;
//                bdtree::logical_table_cache<uint64_t, uint64_t> cache(bdtree::logical_pointer_table{pointer_table_id}, bdtree::node_table{node_table_id});
                bdtree::map<uint64_t, uint64_t> map(cache0, bdtree::get_next_tx_id());
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

        alloc.reset(new awesome::allocator());
        bdtree::map<uint64_t, uint64_t> map(cache0, bdtree::get_next_tx_id());
        auto iter = map.find(1);
        auto end = decltype(iter)();
        assert(iter != end);
        uint64_t i = 1;
        for (;iter != end; ++iter) {
            auto key = *iter;
            assert(key.first == i);
            ++i;
        }
    }
    alloc.reset(nullptr);
    alloc.reset(nullptr);
    alloc.reset(nullptr);
    alloc.reset(new awesome::allocator());
    {
        // test deletion
        std::atomic_size_t deleted_count(0);
        std::atomic_size_t counter(3);
        std::vector<std::thread> threads;
        threads.reserve(2);
        for (uint64_t thread_id = 0; thread_id < 2; ++thread_id) {
            threads.emplace_back([thread_id, pointer_table_id, node_table_id, &counter, &cache0, &deleted_count](){
                awesome::allocator alloc;
                bdtree::logical_table_cache<uint64_t, uint64_t> cache(bdtree::logical_pointer_table{pointer_table_id}, bdtree::node_table{node_table_id});
                bdtree::map<uint64_t, uint64_t> map(cache, bdtree::get_next_tx_id());
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
        alloc.reset(new awesome::allocator());
        assert(deleted_count == 3333);
        bdtree::logical_table_cache<uint64_t, uint64_t> cache(bdtree::logical_pointer_table{pointer_table_id}, bdtree::node_table{node_table_id});
        bdtree::map<uint64_t, uint64_t> map(cache, bdtree::get_next_tx_id());
        for (uint64_t i = 1; i <= 10000; i++) {
            auto iter = map.find(i);
            bool found = iter->first == i;
            if (found) assert(i % 3);
            else assert(i % 3 == 0);
        }
    }

    alloc.reset(new awesome::allocator());
    bdtree::logical_table_cache<uint64_t, uint64_t> cache(bdtree::logical_pointer_table{pointer_table_id}, bdtree::node_table{node_table_id});
    bdtree::map<uint64_t, uint64_t> map(cache, bdtree::get_next_tx_id());
    for (uint64_t i = 1; i <= 1; i++) {
        auto iter = map.find(1);
        bool found = iter->first == 1;
        assert(found);
        map.erase(1);
        map.insert(1,1000);
    }
    map.print_statistics();
    };

//    alloc.reset(new awesome::allocator());
//    bdtree::logical_table_cache<uint64_t, uint64_t> cache(bdtree::logical_pointer_table{pointer_table_id}, bdtree::node_table{node_table_id});
//    bdtree::map<uint64_t, uint64_t> map(cache, bdtree::get_next_tx_id());
//    for (uint64_t i = 1; i <= 1000; i++) {
//        map.insert(i,1000+i);
//    }
//    map.print_statistics();

//    alloc.reset(new awesome::allocator());
//    for (uint64_t i = 1; i <= 1000; i++) {
//        map.erase(i*3);
//    }
//    map.print_statistics();
//    };

    awesome::destroy();
    stop_ram_cloud();
    return 0;
}
