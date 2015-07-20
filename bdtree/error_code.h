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
