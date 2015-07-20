###################
# Bd-Tree
###################
set(BDTREE_SRCS
    bdtree.cpp
)

set(BDTREE_PUBLIC_HDR
    acache.h
    base_backend.h
    base_types.h
    bdtree.h
    deltas.h
    error_code.h
    forward_declarations.h
    iterator.h
    leaf_operations.h
    logical_table_cache.h
    merge_operation.h
    node_pointer.h
    nodes.h
    primitive_types.h
    resolve_operation.h
    search_operation.h
    serialize_policies.h
    serializer.h
    split_operation.h
    stl_specializations.h
    util.h
)

# Transform public header list to use absolute paths
foreach(hdr ${BDTREE_PUBLIC_HDR})
    list(APPEND BDTREE_PUBLIC_HDR_ABSOLUTE ${PROJECT_SOURCE_DIR}/bdtree/${hdr})
endforeach()

# Add Bd-Tree library
add_library(bdtree ${BDTREE_SRCS} ${BDTREE_PUBLIC_HDR_ABSOLUTE})
set_target_properties(bdtree PROPERTIES PUBLIC_HEADER "${BDTREE_PUBLIC_HDR_ABSOLUTE};${PROJECT_BINARY_DIR}/config.h")
target_include_directories(bdtree PUBLIC $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${INCLUDE_INSTALL_DIR}>
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}> $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>)

# Link against Boost
target_include_directories(bdtree PUBLIC ${Boost_INCLUDE_DIRS})

# Link against Crossbow
target_include_directories(bdtree PRIVATE ${Crossbow_INCLUDE_DIRS})
target_link_libraries(bdtree PUBLIC crossbow_allocator)

# Link against TBB
target_include_directories(bdtree PUBLIC ${TBB_INCLUDE_DIRS})
target_link_libraries(bdtree PUBLIC ${TBB_LIBRARIES})

# Install the library
install(TARGETS bdtree EXPORT BdTreeTargets
        ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
        LIBRARY DESTINATION ${LIB_INSTALL_DIR}
        PUBLIC_HEADER DESTINATION ${INCLUDE_INSTALL_DIR}/bdtree)

# Install the cmake target
install(EXPORT BdTreeTargets DESTINATION ${CMAKE_INSTALL_DIR})
