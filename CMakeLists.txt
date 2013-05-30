# Copyright (c) 2006, 2010, Oracle and/or its affiliates. All rights reserved.
# 
# This program is free software; you can kvstoretribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
project(bdtree)
cmake_minimum_required(VERSION 2.8)

set(CONSOLIDATE_AT "10" CACHE STRING "Number of delta nodes allowed in leaf level")
set(MAX_NODE_SIZE "2048" CACHE STRING "Maximal size of a node")
set(MIN_NODE_SIZE "1024" CACHE STRING "Minimal size of a node")
set(BDTREE_FOR_POINT_QUERY TRUE CACHE BOOL "Use the Bd-Tree for point queries")


SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake)

if(APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -stdlib=libc++ -Wconversion -fno-omit-frame-pointer -march=native")
else(APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wconversion -fno-omit-frame-pointer -pthread -march=native")
endif(APPLE)

set(UTIL_DIR "${CMAKE_SOURCE_DIR}/../util")
message(STATUS "Set the util directory to ${UTIL_DIR}")
include_directories(${UTIL_DIR})

set(RAMCLOUD_DIR "${CMAKE_SOURCE_DIR}/../../ramcloud" CACHE PATH "Path to the RamCloud source directory")
set(RAMCLOUD_BINARY_DIR "${RAMCLOUD_DIR}/obj.master" CACHE PATH "Path to the RAMCloud binary directory")
message(STATUS "Looking for RamCloud in ${RAMCLOUD_DIR}")

include_directories(${RAMCLOUD_BINARY_DIR})
include_directories(${RAMCLOUD_DIR}/src)
include_directories(${RAMCLOUD_DIR}/logcabin)
include_directories(${RAMCLOUD_DIR}/gtest/include)

set(CRAMCLOUD_DIR "${CMAKE_SOURCE_DIR}/../cramcloud" CACHE PATH "Path to the C-RamCloud source directory")
set(CRAMCLOUD_BINARY_DIR "${CRAMCLOUD_DIR}/build" CACHE PATH "Path to the C-RAMCloud binary directory")
message(STATUS "Looking for CRamCloud in ${CRAMCLOUD_DIR}")
include_directories(${CRAMCLOUD_DIR})

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CRAMCLOUD_DIR}/cmake)
find_package(Protobuf REQUIRED)
find_package(Ibverbs REQUIRED)
find_package(PCRECPP REQUIRED)

configure_file(bdtree/config.h.cmake bdtree/config.h)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(.)

#add_definitions(-DDBG_B_TREE)

find_package(Boost 1.49 REQUIRED COMPONENTS system thread filesystem)
find_package(MongoDB)
if (MongoDB_FOUND)
    find_package(OpenSSL)
    include_directories(${MongoDB_INCLUDE_DIR})
endif (MongoDB_FOUND)
configure_file(server_config.h.cmake server_config.h)

set(SRCS bdtree.h
    bdtree/serialize_policies.h
    bdtree/primitive_types.h
    bdtree/forward_declarations.h
    bdtree/base_types.h
    bdtree/stl_specializations.h
    bdtree/node_pointer.h
    bdtree/logical_table_cache.h
    bdtree/deltas.h
    bdtree/nodes.h
    bdtree/resolve_operation.h
    bdtree/iterator.h
    bdtree/search_operation.h
    bdtree/leaf_operations.h
    bdtree/split_operation.h
    bdtree/merge_operation.h
    bdtree/acache.h
    bdtree.cc
    amalloc.h
    amalloc.cc
    )

set(TEST_SRCS ${SRCS} test.cc)
set(RAND_TEST_SRCS ${SRCS} rand_test.cc)
set(BDTREE_SERVER_SRCS ${SRCS}
    bdtree_server.cc
    bdtree_server_proc.h
    bdtree_server_proc.cc
    mongo_impl.h
    mongo_impl.cc
    bdtree_db_impl.h
    bdtree_db_impl.cc)

add_executable(bdtree_test ${TEST_SRCS})
#add_executable(rand_test ${RAND_TEST_SRCS})
add_executable(bdtree_server ${BDTREE_SERVER_SRCS})
set(to_link
    pthread
            ${Boost_LIBRARIES}
            ${MySQL_LIBRARY}
            ${MySQLpp_LIBRARY}
#            ${CRAMCLOUD_BINARY_DIR}/libdummy_cramcloud.a
            ${CRAMCLOUD_BINARY_DIR}/libcramcloud.a
            ${RAMCLOUD_BINARY_DIR}/libramcloud.a
            ${PROTOBUF_LIBRARIES}
            ${Ibverbs_LIBRARY}
            ${PCRECPP_LIBRARY}
            rt)
target_link_libraries(bdtree_test ${to_link})
if (MongoDB_FOUND)
    set(to_link ${to_link} ${MongoDB_LIBRARIES} ${OPENSSL_LIBRARIES})
endif (MongoDB_FOUND)
target_link_libraries(bdtree_server ${to_link})
