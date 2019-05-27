#include <gtest/gtest.h>

#include "heap_page_cache.h"
#include "tree.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

using namespace std::chrono;

using KeyType = uint64_t;
using ValueType = uint64_t;

TEST(TreeTest, HandleInsert)
{
    char* tmp = tmpnam(NULL);
    srand(time(0));

    bptree::HeapFile heap_file(tmp, true, 4096);
    bptree::HeapPageCache page_cache(&heap_file, 1024);

    bptree::BTree<8, KeyType, ValueType> tree(&page_cache);

    for (int i = 0; i < 20; i++) {
        KeyType k = rand() % 10000;
        tree.insert(k, rand() % 1000000);
    }

    tree.print();
}

TEST(TreeTest, HandleConcurrentInsert)
{
    char* tmp = tmpnam(NULL);
    srand(time(0));

    bptree::HeapFile heap_file(tmp, true, 4096);
    bptree::HeapPageCache page_cache(&heap_file, 1024);

    bptree::BTree<100, KeyType, ValueType> tree(&page_cache);

    high_resolution_clock::time_point t1 = high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (int i = 0; i < 10; i++) {
        threads.emplace_back([i, &tree]() {
            for (int j = 0; j < 1000; j++) {
                tree.insert(i * 1000 + j, j);
            }
        });
    }

    for (auto&& p : threads) {
        p.join();
    }

    high_resolution_clock::time_point t2 = high_resolution_clock::now();

    std::vector<ValueType> values;
    for (int i = 0; i < 10000; i++) {
        values.clear();
        tree.get_value(i, values);
        EXPECT_EQ(values.size(), 1);
        if (values.size() == 1) {
            EXPECT_EQ(values.front(), i % 1000);
        }
    }

    high_resolution_clock::time_point t3 = high_resolution_clock::now();

    LOG(INFO) << "insert: " << duration_cast<duration<double>>(t2 - t1).count()
              << "s, query: "
              << duration_cast<duration<double>>(t3 - t2).count() << "s";
}
