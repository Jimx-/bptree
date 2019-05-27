cmake_minimum_required(VERSION 3.5)
project(bptree)

set(CMAKE_CXX_STANDARD 17)

set(TOPDIR ${PROJECT_SOURCE_DIR})

include_directories(
    include
    3rdparty/easyloggingpp/src
)

add_subdirectory(3rdparty/googletest/googletest)
enable_testing()
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

set(SOURCE_FILES
    src/heap_file.cpp
    src/heap_page_cache.cpp
    src/tree.cpp
    src/tree_node.cpp)
            
set(HEADER_FILES
    include/heap_file.h 
    include/heap_page_cache.h
    include/mem_page_cache.h
    include/page.h
    include/page_cache.h
    include/tree_node.h)

set(EXT_SOURCE_FILES 
    3rdparty/easyloggingpp/src/easylogging++.cc)

set(LIBRARIES
    pthread
)
 
add_library(bptree STATIC ${SOURCE_FILES} ${HEADER_FILES} ${EXT_SOURCE_FILES})
target_link_libraries(bptree ${LIBRARIES}) 

set(TEST_SOURCE_FILES
    tests/main.cpp
    tests/tree_test.cpp)
    
add_executable(unit_tests ${EXT_SOURCE_FILES} ${TEST_SOURCE_FILES})
target_link_libraries(unit_tests bptree gtest gtest_main ${LIBRARIES})
add_test(unit_tests unit_tests)

