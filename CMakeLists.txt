cmake_minimum_required(VERSION 3.5)
project(bptree)

set(CMAKE_CXX_STANDARD 17)

option(BPTREE_BUILD_TESTS "set ON to build library tests" OFF)

set(TOPDIR ${PROJECT_SOURCE_DIR})

include_directories(
    ${TOPDIR}/include
)

set(SOURCE_FILES
    ${TOPDIR}/src/heap_file.cpp
    ${TOPDIR}/src/heap_page_cache.cpp
    ${TOPDIR}/src/tree.cpp
    ${TOPDIR}/src/tree_node.cpp)
            
set(HEADER_FILES
    ${TOPDIR}/include/bptree/heap_file.h 
    ${TOPDIR}/include/bptree/heap_page_cache.h
    ${TOPDIR}/include/bptree/mem_page_cache.h
    ${TOPDIR}/include/bptree/page.h
    ${TOPDIR}/include/bptree/page_cache.h
    ${TOPDIR}/include/bptree/tree_node.h)

set(EXT_SOURCE_FILES )

set(LIBRARIES
    pthread
)
 
add_library(bptree STATIC ${SOURCE_FILES} ${HEADER_FILES} ${EXT_SOURCE_FILES})
target_link_libraries(bptree ${LIBRARIES}) 

if (BPTREE_BUILD_TESTS)
add_subdirectory(${TOPDIR}/3rdparty/googletest/googletest)
enable_testing()
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

set(TEST_SOURCE_FILES
    ${TOPDIR}/tests/tree_test.cpp)
    
add_executable(bptree_unit_tests ${EXT_SOURCE_FILES} ${TEST_SOURCE_FILES})
target_link_libraries(bptree_unit_tests bptree gtest gtest_main ${LIBRARIES})
add_test(bptree_tests bptree_unit_tests)
endif()
