#include "bdtree.h"

namespace bdtree {

std::atomic<uint64_t> tx_id_counter;

uint64_t get_next_tx_id() {
    return ++tx_id_counter;
}

void got_tx_id(uint64_t tx_id) {
    uint64_t old_value = tx_id_counter;
    while (old_value < tx_id)
        tx_id_counter.compare_exchange_strong(old_value, tx_id);
}

uint64_t get_last_tx_id() { return tx_id_counter.load(); }

}

