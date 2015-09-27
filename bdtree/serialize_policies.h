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

#include <bdtree/serializer.h>

#include <boost/optional.hpp>

template<typename Archiver, typename T>
struct size_policy<Archiver, boost::optional<T>>
{
    std::size_t operator() (Archiver& ar, const boost::optional<T>& obj) const
    {
        bool b = (obj != boost::none);
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
        bool b = (obj != boost::none);
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
