cmake_minimum_required(VERSION 2.8.12)
project(BdTree)

set(CONSOLIDATE_AT "0" CACHE STRING "Number of delta nodes allowed in leaf level")
set(MAX_NODE_SIZE "2048" CACHE STRING "Maximal size of a node")
set(MIN_NODE_SIZE "512" CACHE STRING "Minimal size of a node")

# Set default install paths
set(CMAKE_INSTALL_DIR cmake CACHE PATH "Installation directory for CMake files")
set(INCLUDE_INSTALL_DIR include CACHE PATH "Installation directory for header files")
set(LIB_INSTALL_DIR lib CACHE PATH "Installation directory for libraries")

# Set the Bd-Tree directory
set(BdTree_DIR ${CMAKE_CURRENT_BINARY_DIR} CACHE PATH "Path to the Bd-Tree binaries and configuration")

# Set CMake modules path
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Set compile options
# The cx16 flag is required for GCC to enable 128 bit atomics
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -mcx16")

# Find dependencies
find_package(Boost REQUIRED)
find_package(Crossbow COMPONENTS Allocator REQUIRED)
find_package(Threads REQUIRED)
find_package(TBB REQUIRED)

# Create configuration file
configure_file(bdtree/config.h.in ${PROJECT_BINARY_DIR}/bdtree/config.h)

add_subdirectory(src)
add_subdirectory(test)

# Create cmake config file
configure_file(BdTreeConfig.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/BdTreeConfig.cmake @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/BdTreeConfig.cmake DESTINATION ${CMAKE_INSTALL_DIR})
