cmake_minimum_required(VERSION 3.5)
project(bptree)

set(CMAKE_CXX_STANDARD 17)

option(BPTREE_BUILD_TESTS "set ON to build library tests" OFF)

set(TOPDIR ${PROJECT_SOURCE_DIR})

find_package(Boost COMPONENTS thread)
if (NOT Boost_FOUND)
    message(FATAL_ERROR "Fatal error: Boost (version >= 1.55) required.")
else()
    message(STATUS "Setting up BOOST")
    message(STATUS " Includes - ${Boost_INCLUDE_DIRS}")
    message(STATUS " Library  - ${Boost_LIBRARY_DIRS}")
    message(STATUS " boost_thread ${Boost_THREAD_LIBRARY}")
    link_directories(${Boost_LIBRARY_DIRS})
endif (NOT Boost_FOUND)

set(INCLUDE_DIRS
    ${TOPDIR}/include
    ${Boost_INCLUDE_DIRS}
)

include_directories(
    ${INCLUDE_DIRS}
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
    ${Boost_THREAD_LIBRARIES}
)
 
add_library(bptree STATIC ${SOURCE_FILES} ${HEADER_FILES} ${EXT_SOURCE_FILES})
target_link_libraries(bptree ${LIBRARIES})
target_include_directories(bptree PUBLIC ${INCLUDE_DIRS})
install(TARGETS bptree DESTINATION lib)

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
