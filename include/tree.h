#ifndef _TREE_H_
#define _TREE_H_

#include "page_cache.h"
#include "tree_node.h"

#include <cassert>

namespace bptree {

template <unsigned int N, typename K, typename V,
          typename KeyComparator = std::less<K>,
          typename KeyEq = std::equal_to<K>>
class BTree {
public:
    BTree(AbstractPageCache* page_cache) : page_cache(page_cache)
    {
        bool create = !read_metadata();

        if (create) {
            auto page = page_cache->new_page();
            assert(page->get_id() == META_PAGE_ID);

            root =
                create_node<LeafNode<N, K, V, KeyComparator, KeyEq>>(nullptr);
            write_metadata();
        }
    }

    template <
        typename T,
        typename std::enable_if<std::is_base_of<
            BaseNode<K, V, KeyComparator, KeyEq>, T>::value>::type* = nullptr>
    std::unique_ptr<T> create_node(BaseNode<K, V, KeyComparator, KeyEq>* parent)
    {
        auto page = page_cache->new_page();
        auto node = std::make_unique<T>(this, parent, page->get_id());
        page_cache->unpin_page(page, false);

        return node;
    }

    void get_value(const K& key, std::vector<V>& value_list)
    {
        while (true) {
            try {
                value_list.clear();
                auto* root_node = root.get();
                root_node->get_values(key, true, false, nullptr, value_list, 0);
                if (root_node != root.get()) continue;
                break;
            } catch (OLCRestart&) {
                continue;
            }
        }
    }

    void collect_values(const K& key, bool upper_bound,
                        std::vector<K>& key_list, std::vector<V>& value_list)
    {
        while (true) {
            try {
                key_list.clear();
                value_list.clear();
                auto* root_node = root.get();
                root_node->get_values(key, upper_bound, true, &key_list,
                                      value_list, 0);
                if (root_node != root.get()) continue;
                break;
            } catch (OLCRestart&) {
                continue;
            }
        }
    }

    void insert(const K& key, const V& value)
    {
        while (true) {
            try {
                K split_key;
                auto old_root = root.get();
                if (!old_root)
                    continue; /* old_root may be nullptr when another thread is
                                 updating the root node pointer */

                auto root_sibling = old_root->insert(key, value, split_key, 0);

                if (root_sibling) {
                    auto new_root =
                        create_node<InnerNode<N, K, V, KeyComparator, KeyEq>>(
                            nullptr);

                    root->set_parent(new_root.get());
                    root_sibling->set_parent(new_root.get());

                    new_root->set_size(1);
                    new_root->high_key = root_sibling->get_high_key();
                    new_root->keys[0] = split_key;
                    new_root->child_pages[0] = root->get_pid();
                    new_root->child_pages[1] = root_sibling->get_pid();
                    new_root->child_cache[0] = std::move(root);
                    new_root->child_cache[1] = std::move(root_sibling);

                    root = std::move(new_root);
                    write_node(root.get());
                    write_metadata();

                    /* release the lock on the old root */
                    old_root->write_unlock();
                    continue;
                }

                break;
            } catch (OLCRestart&) {
                continue;
            }
        }
    }

    void print() const { root->print(""); } /* for debug purpose */

    std::unique_ptr<BaseNode<K, V, KeyComparator, KeyEq>>
    read_node(BaseNode<K, V, KeyComparator, KeyEq>* parent, PageID pid)
    {
        auto page = page_cache->fetch_page(pid);

        if (!page) {
            return nullptr;
        }
        auto* buf = page->lock();

        uint32_t tag = *reinterpret_cast<uint32_t*>(buf);
        std::unique_ptr<BaseNode<K, V, KeyComparator, KeyEq>> node;

        if (tag == INNER_TAG) {
            node = std::make_unique<InnerNode<N, K, V, KeyComparator, KeyEq>>(
                this, parent, pid);
        } else if (tag == LEAF_TAG) {
            node = std::make_unique<LeafNode<N, K, V, KeyComparator, KeyEq>>(
                this, parent, pid);
        }

        node->deserialize(&buf[sizeof(uint32_t)],
                          page->get_size() - sizeof(uint32_t));

        page->unlock();
        page_cache->unpin_page(page, false);

        return node;
    }

    void write_node(const BaseNode<K, V, KeyComparator, KeyEq>* node)
    {
        auto page = page_cache->fetch_page(node->get_pid());

        if (!page) return;
        auto* buf = page->lock();
        uint32_t tag = node->is_leaf() ? LEAF_TAG : INNER_TAG;

        *reinterpret_cast<uint32_t*>(buf) = tag;
        node->serialize(&buf[sizeof(uint32_t)],
                        page->get_size() - sizeof(uint32_t));

        page->unlock();
        page_cache->unpin_page(page, true);
    }

    /* iterator interface */
    class iterator {
        friend class BTree<N, K, V, KeyComparator, KeyEq>;

    public:
        using self_type = iterator;
        using value_type = std::pair<K, V>;
        using reference = value_type&;
        using pointer = value_type*;
        using iterator_category = std::forward_iterator_tag;
        using difference_type = int;

        self_type operator++()
        {
            self_type i = *this;
            inc();
            return i;
        }
        self_type operator++(int _unused)
        {
            inc();
            return *this;
        }
        reference operator*() { return kvp; }
        pointer operator->() { return &kvp; }
        bool operator==(const self_type& rhs) { return false; }
        bool operator!=(const self_type& rhs) { return true; }
        bool is_end() const { return ended; }

    private:
        std::vector<K> key_buf;
        std::vector<V> value_buf;
        size_t idx;
        value_type kvp;
        bool ended;
        KeyComparator kcmp;

        using container_type = BTree<N, K, V, KeyComparator, KeyEq>;
        container_type* tree;

        iterator(container_type* tree, KeyComparator kcmp = KeyComparator{})
            : tree(tree), kcmp(kcmp)
        {
            ended = false;
            auto first_node = tree->read_node(
                nullptr,
                BTree<N, K, V, KeyComparator, KeyEq>::FIRST_NODE_PAGE_ID);

            if (!first_node) {
                ended = true;
                return;
            }

            auto leaf = static_cast<LeafNode<N, K, V, KeyComparator, KeyEq>*>(
                first_node.get());
            key_buf.clear();
            value_buf.clear();
            std::copy(leaf->keys.begin(), leaf->keys.begin() + leaf->get_size(),
                      std::back_inserter(key_buf));
            std::copy(leaf->values.begin(),
                      leaf->values.begin() + leaf->get_size(),
                      std::back_inserter(value_buf));

            idx = 0;
            if (key_buf.empty()) {
                ended = true;
            } else {
                kvp = std::make_pair(key_buf[idx], value_buf[idx]);
            }
        }

        iterator(container_type* tree, const K& key,
                 KeyComparator kcmp = KeyComparator{})
            : tree(tree), kcmp(kcmp)
        {
            ended = false;
            tree->collect_values(key, true, key_buf, value_buf);
            idx = std::lower_bound(key_buf.begin(), key_buf.end(), key, kcmp) -
                  key_buf.begin();
            if (idx == key_buf.size()) {
                ended = true;
            } else {
                kvp = std::make_pair(key_buf[idx], value_buf[idx]);
            }
        }

        void inc()
        {
            if (ended) return;
            idx++;
            if (idx == key_buf.size()) {
                get_next_batch();
            }
            if (ended) return;
            kvp = std::make_pair(key_buf[idx], value_buf[idx]);
        }

        void get_next_batch()
        {
            K last_key = key_buf.back();
            tree->collect_values(last_key, false, key_buf, value_buf);
            idx = std::upper_bound(key_buf.begin(), key_buf.end(), last_key,
                                   kcmp) -
                  key_buf.begin();
            if (idx == key_buf.size()) {
                ended = true;
            }
        }
    };

private:
    struct Sentinel {
        friend bool operator==(iterator const& it, Sentinel)
        {
            return it.is_end();
        }

        template <class Rhs,
                  std::enable_if_t<!std::is_same<Rhs, Sentinel>{}, int> = 0>
        friend bool operator!=(Rhs const& ptr, Sentinel)
        {
            return !(ptr == Sentinel{});
        }
        friend bool operator==(Sentinel, iterator const& it)
        {
            return it.is_end();
        }
        template <class Lhs,
                  std::enable_if_t<!std::is_same<Lhs, Sentinel>{}, int> = 0>
        friend bool operator!=(Sentinel, Lhs const& ptr)
        {
            return !(Sentinel{} == ptr);
        }
        friend bool operator==(Sentinel, Sentinel) { return true; }
        friend bool operator!=(Sentinel, Sentinel) { return false; }
    };

public:
    iterator begin() { return iterator(this); }
    iterator begin(const K& key) { return iterator(this, key); }
    Sentinel end() const { return Sentinel{}; }

private:
    static const PageID META_PAGE_ID = 1;
    static const PageID FIRST_NODE_PAGE_ID = META_PAGE_ID + 1;
    static const uint32_t META_PAGE_MAGIC = 0x00C0FFEE;
    static const uint32_t INNER_TAG = 1;
    static const uint32_t LEAF_TAG = 2;

    AbstractPageCache* page_cache;
    std::unique_ptr<BaseNode<K, V, KeyComparator, KeyEq>> root;

    /* metadata: | magic(4 bytes) | root page id(4 bytes) | */
    bool read_metadata()
    {
        auto page = page_cache->fetch_page(META_PAGE_ID);
        if (!page) return false;

        auto* buf = page->lock();
        buf += sizeof(uint32_t);
        PageID root_pid = (PageID) * reinterpret_cast<uint32_t*>(buf);
        root = read_node(nullptr, root_pid);

        page->unlock();
        page_cache->unpin_page(page, false);

        return true;
    }

    void write_metadata()
    {
        auto page = page_cache->fetch_page(META_PAGE_ID);
        auto* buf = page->lock();

        *reinterpret_cast<uint32_t*>(buf) = META_PAGE_MAGIC;
        buf += sizeof(uint32_t);
        *reinterpret_cast<uint32_t*>(buf) = (uint32_t)root->get_pid();
        buf += sizeof(uint32_t);

        page->unlock();
        page_cache->unpin_page(page, true);
    }
};

} // namespace bptree

#endif
