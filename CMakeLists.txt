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


SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake)

if(APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -stdlib=libc++ -Wconversion -fno-omit-frame-pointer -march=native")
else(APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wconversion -fno-omit-frame-pointer -pthread -march=native")
endif(APPLE)

set(UTIL_DIR "${CMAKE_SOURCE_DIR}/../util")
message(STATUS "Set the util directory to ${UTIL_DIR}")
include_directories(${UTIL_DIR})

configure_file(bdtree/config.h.cmake bdtree/config.h)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(.)

#add_definitions(-DDBG_B_TREE)

find_package(Boost 1.48 REQUIRED COMPONENTS system thread filesystem)
find_package(MongoDB)
include_directories(${MongoDB_INCLUDE_DIR})

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
    cramcloud.h
    dummy_cramcloud.cc
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
add_executable(rand_test ${RAND_TEST_SRCS})
add_executable(bdtree_server ${BDTREE_SERVER_SRCS})
target_link_libraries(bdtree_server ${MongoDB_LIBRARIES} ${Boost_LIBRARIES})
