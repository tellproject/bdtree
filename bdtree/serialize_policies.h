#pragma once

#include <serializer.h>
#include <boost/optional.hpp>

template<typename Archiver, typename T>
struct size_policy<Archiver, boost::optional<T>>
{
    std::size_t operator() (Archiver& ar, const boost::optional<T>& obj) const
    {
        bool b = obj;
        ar & b;
        if (b) {
            ar & *obj;
        }
        return 0;
    }
};

template<typename Archiver, typename T>
struct serialize_policy<Archiver, boost::optional<T>>
{
    uint8_t* operator() (Archiver& ar, const boost::optional<T>& obj, uint8_t* pos) {
        bool b = obj;
        ar & b;
        if (b) {
            ar & *obj;
        }
        return ar.pos;
    }
};

template<typename Archiver, typename T>
struct deserialize_policy<Archiver, boost::optional<T>>
{
    const uint8_t* operator() (Archiver& ar, boost::optional<T>& p, const uint8_t* ptr) const
    {
        bool is_set;
        ar & is_set;
        if (is_set) {
            T res;
            ar & res;
            p = res;
        }
        return ar.pos;
    }
};