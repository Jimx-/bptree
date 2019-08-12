# BPTree
A thread-safe B+ tree implementation with disk-based storage that supports millions of key-value pairs

## Example
```c
// create a page cache that allocates pages from a heap file
bptree::HeapPageCache page_cache("/tmp/tree.heap", true, 4096);
// create B+ tree of order 256 whose keys and values are int
// for other key and value types, you can provide custom serializers
// through the KeySerializer and the ValueSerializer interface
bptree::BTree<256, int, int> tree(&page_cache);

// insert key-value pairs
for (int i = 0; i < 100; i++) {
    tree.insert(1, 100);
}

// point search
std::vector<int> values;
tree.get_value(50, values);

// range search
for (auto it = tree.begin(50); it != tree.end(); it++) {
    std::cout << it.first << " " << it.second << std::endl;
}

// also
for (auto&& p : tree) {
    std::cout << p.first << " " << p.second << std::endl;
}
```

## Performance
On Intel Xeon W-2123 with 16GB RAM, the B+ tree supports 0.35 million concurrent writes and 51.4 millions concurrent reads with 10 threads
