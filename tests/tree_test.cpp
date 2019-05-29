#include <gtest/gtest.h>

#include "bptree/heap_page_cache.h"
#include "bptree/mem_page_cache.h"
#include "bptree/tree.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
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

    for (int i = 0; i < 10; i++) {
        KeyType k = rand() % 10000;
        tree.insert(k, rand() % 1000000);
    }

    std::cout << tree;
}

TEST(TreeTest, HandleConcurrentInsert)
{
    bptree::MemPageCache page_cache(4096);
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

    std::cout << "insert: " << duration_cast<duration<double>>(t2 - t1).count()
              << "s, query: "
              << duration_cast<duration<double>>(t3 - t2).count() << "s"
              << std::endl;
}

TEST(TreeTest, TreeIterator)
{
    bptree::MemPageCache page_cache(4096);
    bptree::BTree<100, KeyType, ValueType> tree(&page_cache);

    unsigned long long sum1, sum2;
    sum1 = sum2 = 0;

    for (int i = 0; i < 1000; i++) {
        tree.insert(i, i);
        sum1 += i;
    }

    for (auto&& p : tree) {
        sum2 += p.first;
    }

    EXPECT_EQ(sum1, sum2);
}
