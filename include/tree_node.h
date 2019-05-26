#ifndef _TREE_NODE_H_
#define _TREE_NODE_H_

#include "page.h"

#include "easylogging++.h"

#include <functional>
#include <memory>
#include <vector>

namespace bptree {

template <unsigned int N, typename K, typename V, typename KeyComparator,
          typename KeyEq>
class BTree;

template <typename K, typename V, typename KeyComparator, typename KeyEq>
class BaseNode {
public:
    BaseNode(BaseNode* parent, PageID pid, KeyComparator kcmp = KeyComparator{},
             KeyEq keq = KeyEq{})
        : pid(pid), parent(parent), kcmp(kcmp), keq(keq), size(0)
    {}

    PageID get_pid() const { return pid; }
    void set_pid(PageID id) { pid = id; }
    virtual bool is_leaf() const { return false; }

    BaseNode* get_parent() const { return parent; }
    void set_parent(BaseNode* parent) { this->parent = parent; }
    size_t get_size() const { return size; }
    void set_size(size_t size) { this->size = size; }

    virtual void serialize(uint8_t* buf, size_t size) const = 0;
    virtual void deserialize(const uint8_t* buf, size_t size) = 0;

    virtual void get_value(const K& key, std::vector<V>& value_list) = 0;
    virtual std::unique_ptr<BaseNode> insert(const K& key, const V& val,
                                             K& split_key) = 0;

    virtual void
    print(const std::string& padding = "") = 0; /* for debug purpose */

protected:
    size_t size;
    BaseNode* parent;
    PageID pid;
    KeyComparator kcmp;
    KeyEq keq;
};

template <unsigned int N, typename K, typename V, typename KeyComparator,
          typename KeyEq>
class LeafNode;

template <unsigned int N, typename K, typename V,
          typename KeyComparator = std::less<K>,
          typename KeyEq = std::equal_to<K>>
class InnerNode : public BaseNode<K, V, KeyComparator, KeyEq> {
    friend class LeafNode<N, K, V, KeyComparator, KeyEq>;
    friend class BTree<N, K, V, KeyComparator, KeyEq>;

public:
    InnerNode(BTree<N, K, V, KeyComparator, KeyEq>* tree,
              BaseNode<K, V, KeyComparator, KeyEq>* parent,
              PageID pid = Page::INVALID_PAGE_ID,
              KeyComparator kcmp = KeyComparator{})
        : BaseNode<K, V, KeyComparator, KeyEq>(parent, pid), tree(tree)
    {
        for (int i = 0; i < N + 1; i++) {
            child_pages[i] = Page::INVALID_PAGE_ID;
        }
    }

    BaseNode<K, V, KeyComparator, KeyEq>* get_child(int idx)
    {
        if (child_cache[idx]) {
            /* child in cache */
            return child_cache[idx].get();
        }

        if (child_pages[idx] != Page::INVALID_PAGE_ID) {
            /* read child from page cache */
            child_cache[idx] = tree->read_node(this, child_pages[idx]);
            return child_cache[idx].get();
        }

        return nullptr;
    }

    virtual void serialize(uint8_t* buf, size_t size) const
    {
        /* | size | keys | child_pages | */
        *reinterpret_cast<uint32_t*>(buf) = (uint32_t)this->size;
        buf += sizeof(uint32_t);
        ::memcpy(buf, keys.begin(), sizeof(K) * (N - 1));
        buf += sizeof(K) * (N - 1);
        ::memcpy(buf, child_pages.begin(), sizeof(PageID) * N);
        buf += sizeof(PageID) * N;
    }
    virtual void deserialize(const uint8_t* buf, size_t size)
    {
        this->size = (size_t) * reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);
        ::memcpy(keys.begin(), buf, sizeof(K) * (N - 1));
        buf += sizeof(K) * (N - 1);
        ::memcpy(child_pages.begin(), buf, sizeof(PageID) * N);
        buf += sizeof(PageID) * N;
        for (auto&& p : child_cache) {
            p.reset();
        }
    }

    virtual void get_value(const K& key, std::vector<V>& value_list)
    {
        /* direct the search to the child */
        auto it = std::upper_bound(keys.begin(), keys.begin() + this->size, key,
                                   this->kcmp);
        int child_idx = it - keys.begin();
        auto child = get_child(child_idx);
        if (!child) return;

        child->get_value(key, value_list);
    }

    virtual std::unique_ptr<BaseNode<K, V, KeyComparator, KeyEq>>
    insert(const K& key, const V& val, K& split_key)
    {
        auto it = std::upper_bound(keys.begin(), keys.begin() + this->size, key,
                                   this->kcmp);
        int child_idx = it - keys.begin();
        auto child = get_child(child_idx);
        auto new_child = child->insert(key, val, split_key);

        if (!new_child) return nullptr; /* child did not split */

        ::memmove(&keys[child_idx + 1], &keys[child_idx],
                  (this->size - child_idx) * sizeof(K));
        ::memmove(&child_pages[child_idx + 2], &child_pages[child_idx + 1],
                  (this->size - child_idx) * sizeof(PageID));
        for (size_t i = this->size; i > child_idx; i--) {
            child_cache[i + 1] = std::move(child_cache[i]);
        }

        keys[child_idx] = split_key;
        child_pages[child_idx + 1] = new_child->get_pid();
        child_cache[child_idx + 1] = std::move(new_child);

        this->size++;
        if (this->size < N) return nullptr;

        /* need split */
        auto right_sibling = tree->template create_node<
            InnerNode<N, K, V, KeyComparator, KeyEq>>(this->parent);

        right_sibling->size = this->size - N / 2 - 1;

        ::memcpy(right_sibling->keys.begin(), &this->keys[N / 2 + 1],
                 sizeof(K) * right_sibling->size);
        ::memcpy(right_sibling->child_pages.begin(),
                 &this->child_pages[N / 2 + 1],
                 sizeof(PageID) * (1 + right_sibling->size));

        for (size_t i = N / 2 + 1, j = 0; i < this->size; i++, j++) {
            right_sibling->child_cache[j] = std::move(this->child_cache[i]);
        }

        right_sibling->child_cache[right_sibling->size] =
            std::move(this->child_cache[this->size]);

        split_key = this->keys[N / 2];
        this->size = N / 2;

        tree->write_node(this);
        tree->write_node(right_sibling.get());

        return right_sibling;
    }

    virtual void print(const std::string& padding = "")
    {
        this->get_child(0)->print(padding + "    ");
        for (int i = 0; i < this->size; i++) {
            LOG(INFO) << padding << keys[i];
            this->get_child(i + 1)->print(padding + "    ");
        }
    }

private:
    BTree<N, K, V, KeyComparator, KeyEq>* tree;
    std::array<K, N> keys;                 /* actual size is N - 1 */
    std::array<PageID, N + 1> child_pages; /* actual size is N */
    std::array<std::unique_ptr<BaseNode<K, V, KeyComparator, KeyEq>>, N + 1>
        child_cache; /* actual size is N */
};

template <unsigned int N, typename K, typename V,
          typename KeyComparator = std::less<K>,
          typename KeyEq = std::equal_to<K>>
class LeafNode : public BaseNode<K, V, KeyComparator, KeyEq> {
    friend class InnerNode<N, K, V, KeyComparator, KeyEq>;
    friend class BTree<N, K, V, KeyComparator, KeyEq>;

public:
    LeafNode(BTree<N, K, V, KeyComparator, KeyEq>* tree,
             BaseNode<K, V, KeyComparator, KeyEq>* parent,
             PageID pid = Page::INVALID_PAGE_ID,
             KeyComparator kcmp = KeyComparator{})
        : BaseNode<K, V, KeyComparator, KeyEq>(parent, pid, kcmp), tree(tree)
    {}

    virtual bool is_leaf() const { return true; }

    virtual void serialize(uint8_t* buf, size_t size) const
    {
        /* | size | keys | child_pages | */
        *reinterpret_cast<uint32_t*>(buf) = (uint32_t)this->size;
        buf += sizeof(uint32_t);
        ::memcpy(buf, keys.begin(), sizeof(K) * (N - 1));
        buf += sizeof(K) * (N - 1);
        ::memcpy(buf, values.begin(), sizeof(V) * (N - 1));
        buf += sizeof(V) * (N - 1);
    }
    virtual void deserialize(const uint8_t* buf, size_t size)
    {
        this->size = (size_t) * reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);
        ::memcpy(keys.begin(), buf, sizeof(K) * (N - 1));
        buf += sizeof(K) * (N - 1);
        ::memcpy(values.begin(), buf, sizeof(V) * (N - 1));
        buf += sizeof(V) * (N - 1);
    }

    virtual void get_value(const K& key, std::vector<V>& value_list)
    {
        auto lower = std::lower_bound(keys.begin(), keys.begin() + this->size,
                                      key, this->kcmp);

        if (lower == keys.begin() + this->size) return;

        auto upper = lower;
        while (this->keq(key, *upper))
            upper++;

        std::copy(&values[lower - keys.begin()], &values[upper - keys.begin()],
                  std::back_inserter(value_list));
    }

    virtual std::unique_ptr<BaseNode<K, V, KeyComparator, KeyEq>>
    insert(const K& key, const V& val, K& split_key)
    {
        if (this->size == 0) { /* empty leaf node */
            keys[0] = key;
            values[0] = val;
        } else {
            auto it = std::upper_bound(keys.begin(), keys.begin() + this->size,
                                       key, this->kcmp);
            size_t pos = it - keys.begin();

            ::memmove(it + 1, it, (this->size - pos) * sizeof(K));
            ::memmove(&values[pos + 1], &values[pos],
                      (this->size - pos) * sizeof(V));

            keys[pos] = key;
            values[pos] = val;
        }

        this->size++;
        if (this->size < N) {
            tree->write_node(this);
            return nullptr;
        }

        /* need split */
        auto right_sibling =
            tree->template create_node<LeafNode<N, K, V, KeyComparator, KeyEq>>(
                this->parent);

        right_sibling->size = this->size - N / 2;

        ::memcpy(right_sibling->keys.begin(), &this->keys[N / 2],
                 right_sibling->size * sizeof(K));
        ::memcpy(right_sibling->values.begin(), &this->values[N / 2],
                 right_sibling->size * sizeof(V));

        split_key = this->keys[N / 2];
        this->size = N / 2;

        tree->write_node(this);
        tree->write_node(right_sibling.get());

        return right_sibling;
    }

    virtual void print(const std::string& padding = "")
    {
        LOG(INFO) << padding << "Page ID: " << this->get_pid();
        for (int i = 0; i < this->size; i++) {
            LOG(INFO) << padding << keys[i] << " -> " << values[i];
        }
    }

private:
    BTree<N, K, V, KeyComparator, KeyEq>* tree;
    std::array<K, N> keys;   /* actual size is N - 1 */
    std::array<V, N> values; /* actual size is N - 1 */
};

} // namespace bptree

#endif
