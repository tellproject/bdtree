#pragma once
#include <boost/optional.hpp>
#include <vector>

#include "primitive_types.h"
#include "base_types.h"
#include "../amalloc.h"

namespace bdtree {
	template<typename Key, typename Value>
    struct inner_node_t : public node<Key, Value> {
        typedef Key key_type;
        typedef Value value_type;
        typedef node<Key, Value> type;
        static constexpr node_type_t node_type = node_type_t::InnerNode;
    public: // modifying functions
        void clear_deltas() {
            //stub to be API compatible with leaf_node_t
        }
        void set_pptr(physical_pointer pptr){
            //stub to be API compatible with leaf_node_t
        }
        void set_level(int8_t l) {
            level = l;
        }

    public: // construction/destruction
        inner_node_t(physical_pointer pptr) {}
        ~inner_node_t() {}
    public: // data
        std::vector<std::pair<key_type, logical_pointer> > array_;
        key_type low_key_;
        boost::optional<key_type> high_key_;
        logical_pointer right_link_ = {0};
        int8_t level = -1;
    public: // visitors
        template<typename Archiver>
        void visit(Archiver& ar) {
            ar & array_;
            ar & low_key_;
            ar & high_key_;
            ar & right_link_;
            ar & level;
            assert(level >= 0);
            assert(bool(high_key_) == bool(right_link_.value));
            assert(low_key_ != high_key_);
            assert(!high_key_ || low_key_ < *high_key_);
#ifndef NDEBUG
            for (size_t i=0; i< array_.size(); ++i) {
                if (i > 0)
                    assert(array_[i-1].first < array_[i].first);
                auto & key = array_[i];
                assert(!high_key_ || (!(key.first > *high_key_) && !(key.first == *high_key_)));
                assert(!(key.first < low_key_));
            }
#endif
        }
    };
    
    template<typename Key, typename Value>
    struct leaf_node_t : public node<Key, Value> {
    public:
        typedef Key key_type;
        typedef Value value_type;
        typedef node<Key, Value> type;
        static constexpr node_type_t node_type = node_type_t::LeafNode;
    public: // modifying functions
        void clear_deltas() {
            deltas_.clear();
        }
        void set_pptr(physical_pointer pptr){
            leaf_pptr_ = pptr;
        }
        void set_level(int8_t l) {
        }
    public: // construction/destruction
        leaf_node_t(physical_pointer pptr) : leaf_pptr_(pptr) {}
        ~leaf_node_t() {}
    public: // data
        physical_pointer leaf_pptr_;//the pointer to the leaf node without deltas
        std::vector<physical_pointer> deltas_;
        std::vector<std::pair<key_type, Value> > array_;
        key_type low_key_;
        boost::optional<key_type> high_key_;
        logical_pointer right_link_ = {0};
        constexpr static int8_t level = 0;
    public: // serialization
        template<typename Archiver>
        void visit(Archiver& ar) {
            ar & array_;
            ar & low_key_;
            ar & high_key_;
            ar & right_link_;
            assert(bool(high_key_) == bool(right_link_.value));
            assert(low_key_ != high_key_);
            assert(!high_key_ || low_key_ < *high_key_);
#ifndef NDEBUG
            for (size_t i=0; i< array_.size(); ++i) {
                if (i > 0)
                    assert(array_[i-1].first < array_[i].first);
                auto & key = array_[i];
                assert(!high_key_ || (!(key.first > *high_key_) && !(key.first == *high_key_)));
                assert(!(key.first < low_key_));
            }
#endif
        }
    };
}
