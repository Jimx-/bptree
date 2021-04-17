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

    bptree::MemPageCache page_cache(1024);

    bptree::BTree<8, KeyType, ValueType> tree(&page_cache);

    for (int i = 0; i < 10; i++) {
        KeyType k = rand() % 10000;
        tree.insert(k, rand() % 1000000);
    }

    std::cout << tree;
}

TEST(TreeTest, HandleInsert2)
{
    const int N = 1000000;
    bptree::MemPageCache page_cache(4096);
    bptree::BTree<100, KeyType, ValueType> tree(&page_cache);

    for (int i = 0; i < N; i++) {
        tree.insert(i, i + 1);
    }

    for (int i = 0; i < N; i++) {
        std::vector<ValueType> values;
        tree.get_value(i, values);
        EXPECT_EQ(values.size(), 1);
        EXPECT_EQ(values.front(), i + 1);
    }
}

TEST(TreeTest, HandleConcurrentInsert)
{
    // bptree::MemPageCache page_cache(4096);
    // bptree::HeapPageCache page_cache(tmpnam(nullptr), true, 4096);
    const int N = 1000;
    bptree::MemPageCache page_cache(4096);
    bptree::BTree<256, KeyType, ValueType> tree(&page_cache);

    high_resolution_clock::time_point t1 = high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (int i = 0; i < 10; i++) {
        threads.emplace_back([i, &tree]() {
            for (int j = 0; j < N; j++) {
                tree.insert(i * N + j, j);
            }
        });
    }

    for (auto&& p : threads) {
        p.join();
    }

    high_resolution_clock::time_point t2 = high_resolution_clock::now();

    threads.clear();
    for (int i = 0; i < 10; i++) {
        threads.emplace_back([i, &tree]() {
            std::vector<ValueType> values;
            for (int j = 0; j < N; j++) {
                values.clear();
                tree.get_value(i * N + j, values);
                EXPECT_EQ(values.size(), 1);
                if (values.size() == 1) {
                    EXPECT_EQ(values.front(), j);
                }
            }
        });
    }

    for (auto&& p : threads) {
        p.join();
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

    tree.print(std::cout);
    for (auto&& p : tree) {
        std::cout << p.first << ", ";
        sum2 += p.first;
    }
    std::cout << std::endl;

    EXPECT_EQ(sum1, sum2);
}
