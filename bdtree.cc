#include "bdtree.h"

namespace bdtree {

std::atomic<uint64_t> tx_id_counter;
    
uint64_t get_next_tx_id() {
    return ++tx_id_counter;
}
    
uint64_t get_last_tx_id() { return tx_id_counter.load(); }

}

