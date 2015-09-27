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
#include <cstring>
#include <type_traits>
#include <vector>
#include <string>
#include <memory>
#include <cassert>

struct dummy {
    template<typename T>
    dummy& operator& (const T& obj) {
        return *this;
    }
};


template<typename Type>
struct has_visit_helper {
    class yes { char m;};
    class no { yes m[2];};
    struct BaseMixin
    {
        void visit(){}
    };
    struct Base : public Type, public BaseMixin {};
    template <typename T, T t>  class Helper{};
    template <typename U>
    static no deduce(U*, Helper<void (BaseMixin::*)(), &U::visit>* = 0);
    static yes deduce(...);
public:
    static const bool value = sizeof(yes) == sizeof(deduce((Base*)(0)));
};


template<typename T>
struct has_visit {
    struct helper { static constexpr bool value = false; };
    typedef typename std::conditional<std::is_class<T>::value, has_visit_helper<T>, helper>::type value_type;
    static constexpr bool value = value_type::value;
};

template<typename T>
struct serializable : T
{
    template<typename Archiver>
    void visit(Archiver& ar) const {
        const_cast<serializable<T>*>(this)->visit(ar);
    }
    
    using T::visit;
};

template<typename Archiver, typename T>
struct serialize_policy
{
    static_assert(std::is_pod<T>::value, "Default serialize policy can only handle PODs");

    uint8_t* operator() (Archiver&, const T& obj, uint8_t* pos) const
    {
        memcpy(pos, &obj, sizeof(T));
        return pos + sizeof(T);
    }
};

template<typename Archiver>
struct serialize_policy<Archiver, bool>
{
    uint8_t* operator() (Archiver& ar, bool b, uint8_t* pos) const {
        uint8_t r = b ? 1 : 0;
        *pos = r;
        return ++pos;
    }
};

template<typename Archiver>
struct serialize_policy<Archiver, std::string>
{
    uint8_t* operator() (Archiver&, const std::string& obj, uint8_t* pos) const
    {
        uint32_t len = uint32_t(obj.size());
        memcpy(pos, &len, sizeof(std::uint32_t));
        pos += sizeof(std::uint32_t);
        memcpy(pos, obj.data(), len);
        return pos + len;
    }
};

template<typename Archiver, typename T, typename Allocator>
struct serialize_policy<Archiver, std::vector<T, Allocator>>
{
    uint8_t* operator() (Archiver& ar, const std::vector<T, Allocator>& v, uint8_t* pos) const {
        std::size_t s = v.size();
        ar & s;
        for (auto& e : v) {
            ar & e;
        }
        return ar.pos;
    }
};

template <typename Archiver, typename U, typename V>
struct serialize_policy<Archiver, std::pair<U, V>> {
    uint8_t* operator() (Archiver& ar, std::pair<U, V> p, uint8_t* pos) const {
        ar & p.first;
        ar & p.second;
        return ar.pos;
    }
};

template<typename Archiver, typename Tuple, int Index = std::tuple_size<Tuple>::value - 1>
struct serialize_policy_tuple_impl {
    uint8_t* operator() (Archiver& ar, const Tuple& v, uint8_t* pos) const {
        ar & std::get<Index>(v);
        serialize_policy_tuple_impl<Archiver, Tuple, Index -1> ser;
        ser(ar,v,ar.pos);
        return ar.pos;
    }
};

template<typename Archiver, typename Tuple>
struct serialize_policy_tuple_impl<Archiver, Tuple, -1> {
    uint8_t* operator() (Archiver& ar, const Tuple& v, uint8_t* pos) const {
        return pos;
    }
};

template<typename Archiver, typename... T>
struct serialize_policy<Archiver, std::tuple<T...> >
{
    uint8_t* operator() (Archiver& ar, const std::tuple<T...>& v, uint8_t* pos) const {
        serialize_policy_tuple_impl<Archiver, std::tuple<T...> > ser;
        ser(ar,v,pos);
        return ar.pos;
    }
};

template<typename Archiver, typename T>
struct deserialize_policy
{
    static_assert(std::is_pod<T>::value, "Default serialize policy can only handle PODs");

    const uint8_t* operator() (Archiver&, T& out, const uint8_t* ptr) const
    {
        memcpy(&out, ptr, sizeof(T));
        return ptr + sizeof(T);
    }
};

template<typename Archiver>
struct deserialize_policy<Archiver, bool>
{
    const uint8_t* operator() (Archiver& ar, bool& out, const uint8_t* ptr) const
    {
        out = *ptr == 1 ? true : false;
        return ++ptr;
    }
};

template<typename Archiver>
struct deserialize_policy<Archiver, std::string>
{
    const uint8_t* operator() (Archiver&, std::string& out, const uint8_t* ptr) const
    {
        const std::uint32_t s = *reinterpret_cast<const std::uint32_t*>(ptr);
        out = std::string(reinterpret_cast<const char*>(ptr + sizeof(std::uint32_t)), s);
        return ptr + sizeof(s) + s;
    }
};

template<typename Archiver, typename U, typename V>
struct deserialize_policy<Archiver, std::pair<U, V>>
{
    const uint8_t* operator() (Archiver& ar, std::pair<U, V>& p, const uint8_t* ptr) const
    {
        ar & p.first;
        ar & p.second;
        return ar.pos;
    }
};

template<typename Archiver, typename T, typename Allocator>
struct deserialize_policy<Archiver, std::vector<T, Allocator>>
{
    const uint8_t* operator() (Archiver& ar, std::vector<T, Allocator>& out, const uint8_t* ptr) const
    {
        const std::size_t s = *reinterpret_cast<const std::size_t*>(ptr);
        ar.pos = ptr + sizeof(s);
        out.reserve(s);
        for (std::size_t i = 0; i < s; ++i) {
            T obj;
            ar & obj;
            out.push_back(obj);
        }
        return ar.pos;
    }
};

template<typename Archiver, typename Tuple, int Index = std::tuple_size<Tuple>::value - 1>
struct deserialize_policy_tuple_impl {
    const uint8_t* operator() (Archiver& ar, Tuple& v, const uint8_t* pos) const {
        ar & std::get<Index>(v);
        deserialize_policy_tuple_impl<Archiver, Tuple, Index -1> ser;
        ser(ar,v,ar.pos);
        return ar.pos;
    }
};

template<typename Archiver, typename Tuple>
struct deserialize_policy_tuple_impl<Archiver, Tuple, -1> {
    const uint8_t* operator() (Archiver& ar, Tuple& v, const uint8_t* pos) const {
        return pos;
    }
};

template<typename Archiver, typename... T>
struct deserialize_policy<Archiver, std::tuple<T...> >
{
    const uint8_t* operator() (Archiver& ar, std::tuple<T...>& out, const uint8_t* pos) const {
        deserialize_policy_tuple_impl<Archiver, std::tuple<T...> > ser;
        ser(ar,out,pos);
        return ar.pos;
    }
};

template<typename Archiver, typename T>
struct size_policy
{
    static_assert(std::is_pod<T>::value, "Default size policy can only handle PODs");

    std::size_t operator() (Archiver& ar, const T& obj) const
    {
        return sizeof(T);
    }
};

template<typename Archiver>
struct size_policy<Archiver, bool>
{
    std::size_t operator() (Archiver& ar, bool obj) const
    {
        return sizeof(uint8_t);
    }
};

template<typename Archiver>
struct size_policy<Archiver, std::string>
{
    std::size_t operator() (Archiver& ar, const std::string& obj) const
    {
        return sizeof(uint32_t) + obj.size();
    }
};

template<typename Archiver, typename U, typename V>
struct size_policy<Archiver, std::pair<U, V>>
{
    std::size_t operator() (Archiver& ar, const std::pair<U, V>& p) const
    {
        ar & p.first;
        ar & p.second;
        return 0;
    }
};

template<typename Archiver, typename T, typename Allocator>
struct size_policy<Archiver, std::vector<T, Allocator>>
{
    std::size_t operator() (Archiver& ar, const std::vector<T, Allocator>& obj) const
    {
        std::size_t s;
        ar & s;
        for (auto& e : obj) {
            ar & e;
        }
        return 0;
    }
};

template<typename Archiver, typename Tuple, int Index = std::tuple_size<Tuple>::value - 1>
struct size_policy_tuple_impl {
    std::size_t operator() (Archiver& ar, const Tuple& obj) const {
        ar & std::get<Index>(obj);
        size_policy_tuple_impl<Archiver, Tuple, Index -1> ser;
        ser(ar,obj);
        return 0;
    }
};

template<typename Archiver, typename Tuple>
struct size_policy_tuple_impl<Archiver, Tuple, -1> {
    std::size_t operator() (Archiver& ar, const Tuple& obj) const {
        return 0;
    }
};

template<typename Archiver, typename... T>
struct size_policy<Archiver, std::tuple<T...> >
{
    std::size_t operator() (Archiver& ar, const std::tuple<T...>& obj) const {
        size_policy_tuple_impl<Archiver, std::tuple<T...> > ser;
        return ser(ar,obj);
    }
};

struct sizer {
    std::size_t size;
    sizer() : size(0) {}

    template<typename T>
    struct visit_impl {
        static void exec(sizer& s, const T& o) {
            auto& obj = reinterpret_cast<const serializable<T>&>(o);
            obj.visit(s);
        }
    };

    template<typename T>
    struct other_impl {
        static void exec(sizer& s, const T& obj) {
            size_policy<sizer, T> p;
            s.size += p(s, obj);
        }
    };

    template<typename T>
    sizer& operator& (const T& obj) {
        std::conditional<has_visit<T>::value, visit_impl<T>, other_impl<T>>::type::exec(*this, obj);
        return *this;
    }
};

struct serializer {
    std::unique_ptr<uint8_t[]> buffer;
    uint8_t* pos;

    serializer(std::size_t size) : buffer(new uint8_t[size]), pos(buffer.get()) {}

    template<typename T>
    typename std::enable_if<!has_visit<T>::value, serializer&>::type operator& (const T& obj) {
        serialize_policy<serializer, T> ser;
        pos = ser(*this, obj, pos);
        return *this;
    }

    template<typename T>
    typename std::enable_if<has_visit<T>::value, serializer&>::type operator& (const T& o) {
        auto& obj = reinterpret_cast<const serializable<T>&>(o);
        obj.visit(*this);
        return *this;
    }
};

struct serializer_into_array {
    uint8_t* buffer;
    uint8_t* pos;
    
    serializer_into_array(uint8_t* b) : buffer(b), pos(buffer) {}
    
    template<typename T>
    typename std::enable_if<!has_visit<T>::value, serializer_into_array&>::type operator& (const T& obj) {
        serialize_policy<serializer_into_array, T> ser;
        pos = ser(*this, obj, pos);
        return *this;
    }
    
    template<typename T>
    typename std::enable_if<has_visit<T>::value, serializer_into_array&>::type operator& (const T& o) {
        auto& obj = reinterpret_cast<const serializable<T>&>(o);
        obj.visit(*this);
        return *this;
    }
};

struct deserializer {
    const uint8_t* pos;

    deserializer(const uint8_t* buffer) : pos(buffer) {}

    template<typename T>
    typename std::enable_if<!has_visit<T>::value, deserializer&>::type operator& (T& obj) {
        deserialize_policy<deserializer, T> ser;
        pos = ser(*this, obj, pos);
        return *this;
    }

    template<typename T>
    typename std::enable_if<has_visit<T>::value, deserializer&>::type operator& (T& obj) {
        obj.visit(*this);
        return *this;
    }
};

template<typename T>
const uint8_t* deserialize(T& out, const uint8_t* buffer)
{
    deserializer des(buffer);
    des & out;
    return des.pos;
}

template<typename T>
std::size_t serialize(std::unique_ptr<uint8_t[]>& res, const T& obj) {
    sizer s;
    s & obj;
    serializer ser(s.size);
    ser & obj;
    res = std::move(ser.buffer);
    assert(ser.pos == res.get() + s.size);
//#ifndef NDEBUG
//    T* t;
//    assert(deserialize(t, res.get()) == res.get() + s.size);
//#endif
    return s.size;
}
