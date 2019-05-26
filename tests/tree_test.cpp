#include <gtest/gtest.h>

#include "heap_page_cache.h"
#include "tree.h"

#include <cstdio>
#include <cstdlib>

using KeyType = uint64_t;
using ValueType = uint64_t;

TEST(TreeTest, HandleInsert)
{
    char* tmp = tmpnam(NULL);
    srand(time(0));

    bptree::HeapFile heap_file(tmp, true, 4096);
    bptree::HeapPageCache page_cache(&heap_file, 1024);

    bptree::BTree<4, KeyType, ValueType> tree(&page_cache);

    for (int i = 0; i < 10; i++) {
        KeyType k = rand() % 5;
        tree.insert(k, rand() % 1000000);
    }

    tree.print();

    std::vector<ValueType> values;
    tree.get_value(4, values);

    for (auto&& p : values) {
        LOG(INFO) << p;
    }
}
