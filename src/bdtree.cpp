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

