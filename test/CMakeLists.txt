###################
# Unit tests
###################
set(TEST_SRCS
)

set(TEST_PRIVATE_HDR
    dummy_backend.hpp
)

# Add test executable
add_executable(bdtree-tests main.cpp ${TEST_SRCS} ${TEST_PRIVATE_HDR})
target_link_libraries(bdtree-tests PRIVATE bdtree)

# Link against Threads
target_link_libraries(bdtree-tests PUBLIC ${CMAKE_THREAD_LIBS_INIT})

# Link against Crossbow
target_include_directories(bdtree-tests PRIVATE ${Crossbow_INCLUDE_DIRS})

# Link against TBB
target_include_directories(bdtree-tests PUBLIC ${TBB_INCLUDE_DIRS})
target_link_libraries(bdtree-tests PUBLIC ${TBB_LIBRARIES})

add_test(bdtree-tests bdtree-tests)
