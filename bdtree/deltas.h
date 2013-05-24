#pragma once
#include "forward_declarations.h"
#include "base_types.h"

namespace bdtree {

template<typename Key, typename Value>
struct insert_delta_t : public node<Key, Value>
{
    static constexpr node_type_t node_type = node_type_t::InsertDelta;
    std::pair<Key, Value> value;
    physical_pointer next;

    template<typename Archiver>
    void visit(Archiver& ar) {
        ar & this->value;
        ar & this->next;
    }
};

template<typename Key, typename Value>
struct delete_delta_t : public node<Key, Value>
{
    static constexpr node_type_t node_type = node_type_t::DeleteDelta;
    Key key;
    physical_pointer next;

    template<typename Archiver>
    void visit(Archiver& ar) {
        ar & this->key;
        ar & this->next;
    }
};

template<typename Key, typename Value>
struct split_delta_t : public node<Key, Value>
{
    static constexpr node_type_t node_type = node_type_t::SplitDelta;
    physical_pointer next;
    logical_pointer new_right;
    Key right_key;
    int8_t level = -1;

    template<typename Archiver>
    void visit(Archiver& ar) {
        ar & this->next;
        ar & this->new_right;
        ar & this->right_key;
        ar & this->level;
        assert(this->level >= 0);
    }
};

template<typename Key, typename Value>
struct remove_delta_t : public node<Key, Value>
{
    static constexpr node_type_t node_type = node_type_t::RemoveDelta;
    Key low_key;
    physical_pointer next;
    int8_t level = -1;

    template<typename Archiver>
    void visit(Archiver& ar) {
        ar & this->low_key;
        ar & this->next;
        ar & this->level;
        assert(this->level >= 0);
    }
};

template<typename Key, typename Value>
struct merge_delta_t : public node<Key, Value>
{
    static constexpr node_type_t node_type = node_type_t::MergeDelta;
    physical_pointer next;
    logical_pointer rmdelta;
    physical_pointer rmdeltapptr;
    physical_pointer rm_next;
    Key right_low_key;
    int8_t level = -1;

    template<typename Archiver>
    void visit(Archiver& ar) {
        ar & this->right_low_key;
        ar & this->rmdelta;
        ar & this->rmdeltapptr;
        ar & this->next;
        ar & this->rm_next;
        ar & this->level;
        assert(this->level >= 0);
    }
};

}
