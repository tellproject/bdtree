#include "bdtree.h"
#include <memory>
#include <thread>
#include <random>

uint8_t* rc_alloc_fun(size_t s) { return new uint8_t[s]; }
void rc_dealloc_fun(uint8_t* b) { delete[] b; }

int main() {
    init_ram_cloud("", &rc_alloc_fun, &rc_dealloc_fun);
    awesome::init();
    rc_create_table("pointer_table");
    rc_create_table("node_table");
    bdtree::logical_pointer_table pointer_table;
    bdtree::node_table node_table;
    rc_get_table_id(&pointer_table.value, "pointer_table");
    rc_get_table_id(&node_table.value, "node_table");
    std::unique_ptr<awesome::allocator> alloc;
    uint64_t tx_id = 0;
    //populate
    {
        ++tx_id;
        alloc.reset(new awesome::allocator());
        bdtree::logical_table_cache<uint64_t, uint64_t> cache(pointer_table, node_table);
        bdtree::map<uint64_t, uint64_t> map(cache, tx_id, true);
        for (uint64_t i = 0; i < 300000; ++i) {
            if (i % 1000 == 0) std::cout << "Inserted " << i << " values" << std::endl;
            map.insert(i, i);
        }
    }
    std::cout << "Finished population" << std::endl;
    rc_init_rand(1235219, 2);
    std::mt19937_64 mt;
    std::uniform_int_distribution<uint64_t> dist;

    std::thread rand_thread([](){ rc_run_rand(); });
    rand_thread.join();
}
