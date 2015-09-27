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

#include <cstdint>
#include <system_error>

namespace bdtree {
namespace error {

/**
 * @brief Bd-Tree errors triggered while executing an backend operation
 */
enum backend_error {
    object_exists = 1,
    object_doesnt_exist,
    wrong_version
};

class backend_category : public std::error_category {
public:
    const char* name() const noexcept {
        return "bdtree";
    }

    std::string message(int value) const {
        switch (value) {
        case error::object_exists:
            return "Object exists";

        case error::object_doesnt_exist:
            return "Object does not exist";

        case error::wrong_version:
            return "Wrong version";

        default:
            return "bdtree error";
        }
    }
};

inline const std::error_category& get_backend_category() {
    static backend_category instance;
    return instance;
}

inline std::error_code make_error_code(error::backend_error e) {
    return std::error_code(static_cast<int>(e), get_backend_category());
}

inline std::error_condition make_error_condition(error::backend_error e) {
    return std::error_condition(static_cast<int>(e), get_backend_category());
}

} // namespace error
} // namespace bdtree

namespace std {

template<>
struct is_error_condition_enum<bdtree::error::backend_error> : public std::true_type {
};

} // namespace std
