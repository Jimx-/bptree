#ifndef _TREE_NODE_H_
#define _TREE_NODE_H_

#include "page.h"

#include "easylogging++.h"

#include <atomic>
#include <functional>
#include <memory>
#include <vector>

namespace bptree {

class OLCRestart : public std::exception {};

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
    K get_high_key() const { return high_key; }

    virtual void serialize(uint8_t* buf, size_t size) const = 0;
    virtual void deserialize(const uint8_t* buf, size_t size) = 0;

    /* if upper_bound is true, then upper bound is used when directing the
     * search, otherwise lower bound is used. if collect is true, returns all
     * keys and values in the leaf node, otherwise returns only the values that
     * match the key */
    virtual void get_values(const K& key, bool upper_bound, bool collect,
                            std::vector<K>* key_list,
                            std::vector<V>& value_list,
                            uint64_t parent_version) = 0;

    virtual std::unique_ptr<BaseNode> insert(const K& key, const V& val,
                                             K& split_key,
                                             uint64_t parent_version) = 0;

    virtual uint64_t read_lock_or_restart(bool& need_restart)
    {
        uint64_t version = version_counter.load();
        need_restart = is_locked(version) || is_obsolete(version);
        return version;
    }

    virtual uint64_t upgrade_to_write_lock_or_restart(uint64_t version,
                                                      bool& need_restart)
    {
        if (version_counter.compare_exchange_strong(version, version + 0b10)) {
            need_restart = false;
            return version + 0b10;
        }

        need_restart = true;
        return version;
    }

    virtual void write_lock_or_restart(bool& need_restart)
    {
        auto version = read_lock_or_restart(need_restart);
        if (need_restart) return;
        upgrade_to_write_lock_or_restart(version, need_restart);
    }

    virtual void write_unlock() { version_counter.fetch_add(0b10); }
    virtual bool read_unlock_or_restart(uint64_t start_version) const
    {
        return (start_version != version_counter.load());
    }

    virtual void
    print(const std::string& padding = "") = 0; /* for debug purpose */

protected:
    size_t size;
    BaseNode* parent;
    PageID pid;
    KeyComparator kcmp;
    KeyEq keq;
    std::atomic<uint64_t> version_counter;
    K high_key;

    bool is_locked(uint64_t version) const { return (version & 0b10) == 0b10; }
    bool is_obsolete(uint64_t version) const { return (version & 1) == 1; }
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

    BaseNode<K, V, KeyComparator, KeyEq>* get_child(int idx, bool write_locked,
                                                    uint64_t& version)
    {
        if (child_cache[idx]) {
            /* child in cache */
            return child_cache[idx].get();
        }

        if (child_pages[idx] != Page::INVALID_PAGE_ID) {
            /* read child from page cache */
            bool need_restart;
            if (!write_locked) {
                version = this->upgrade_to_write_lock_or_restart(version,
                                                                 need_restart);
                if (need_restart) throw OLCRestart();
            }

            if (!child_cache[idx]) {
                child_cache[idx] = tree->read_node(this, child_pages[idx]);
            }

            this->write_unlock();
            throw OLCRestart();

            /* unreachable */
            return child_cache[idx].get();
        }

        return nullptr;
    }

    virtual void serialize(uint8_t* buf, size_t size) const
    {
        /* | size | keys | child_pages | */
        *reinterpret_cast<uint32_t*>(buf) = (uint32_t)this->size;
        buf += sizeof(uint32_t);
        *reinterpret_cast<K*>(buf) = this->high_key;
        buf += sizeof(K);
        ::memcpy(buf, keys.begin(), sizeof(K) * (N - 1));
        buf += sizeof(K) * (N - 1);
        ::memcpy(buf, child_pages.begin(), sizeof(PageID) * N);
        buf += sizeof(PageID) * N;
    }
    virtual void deserialize(const uint8_t* buf, size_t size)
    {
        this->size = (size_t) * reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);
        this->high_key = *reinterpret_cast<const K*>(buf);
        buf += sizeof(K);
        ::memcpy(keys.begin(), buf, sizeof(K) * (N - 1));
        buf += sizeof(K) * (N - 1);
        ::memcpy(child_pages.begin(), buf, sizeof(PageID) * N);
        buf += sizeof(PageID) * N;
        for (auto&& p : child_cache) {
            p.reset();
        }
    }

    virtual void get_values(const K& key, bool upper_bound, bool collect,
                            std::vector<K>* key_list,
                            std::vector<V>& value_list, uint64_t parent_version)
    {
        uint64_t version;
        bool need_restart;
        version = this->read_lock_or_restart(need_restart);
        if (need_restart) throw OLCRestart();

        if (this->parent &&
            this->parent->read_unlock_or_restart(parent_version)) {
            throw OLCRestart();
        }

        /* direct the search to the child */
        int child_idx =
            std::upper_bound(keys.begin(), keys.begin() + this->size, key,
                             this->kcmp) -
            keys.begin();
        auto child = get_child(child_idx, false, version);
        if (!upper_bound && child_idx != this->size &&
            child->get_high_key() <= key) {
            child_idx++;
            child = get_child(child_idx, false, version);
        }
        if (!child) return;

        if (this->read_unlock_or_restart(version)) throw OLCRestart();

        child->get_values(key, upper_bound, collect, key_list, value_list,
                          version);
    }

    virtual std::unique_ptr<BaseNode<K, V, KeyComparator, KeyEq>>
    insert(const K& key, const V& val, K& split_key, uint64_t parent_version)
    {
        bool need_restart;
        auto version = this->read_lock_or_restart(need_restart);
        if (need_restart) throw OLCRestart();

        if (this->size == N - 1) { /* node is full, do eager split */
            /* upgrade parent's and own lock to write lock */
            if (this->parent) {
                parent_version = this->parent->upgrade_to_write_lock_or_restart(
                    parent_version, need_restart);
                if (need_restart) throw OLCRestart();
            }

            version =
                this->upgrade_to_write_lock_or_restart(version, need_restart);
            if (need_restart) {
                if (this->parent) {
                    this->parent->write_unlock();
                }
                throw OLCRestart();
            }

            /* safe to split now */
            auto right_sibling = tree->template create_node<
                InnerNode<N, K, V, KeyComparator, KeyEq>>(this->parent);

            right_sibling->size = this->size - N / 2 - 1;

            ::memcpy(right_sibling->keys.begin(), &this->keys[N / 2 + 1],
                     sizeof(K) * right_sibling->size);
            ::memcpy(right_sibling->child_pages.begin(),
                     &this->child_pages[N / 2 + 1],
                     sizeof(PageID) * (1 + right_sibling->size));

            for (size_t i = N / 2 + 1, j = 0; i <= this->size; i++, j++) {
                right_sibling->child_cache[j] = std::move(this->child_cache[i]);
                if (right_sibling->child_cache[j]) {
                    right_sibling->child_cache[j]->set_parent(
                        right_sibling.get());
                }
            }

            split_key = this->keys[N / 2];
            this->size = N / 2;

            right_sibling->high_key = this->high_key;
            auto child = get_child(this->size, true, version);
            this->high_key = child->get_high_key();

            tree->write_node(this);
            tree->write_node(right_sibling.get());

            /* if the current node is the root node, the lock is not
             * released until new root is created in BTree::insert() */
            if (this->parent) {
                this->write_unlock();
            }

            /* hold parent's lock until the new sibling node has been
             * inserted into parent node */
            return right_sibling;
        }

        if (this->parent) {
            if (this->parent->read_unlock_or_restart(parent_version))
                throw OLCRestart();
        }

        if (this->high_key < key) {
            /* upgrade the lock and upate high key */
            version =
                this->upgrade_to_write_lock_or_restart(version, need_restart);
            if (need_restart) throw OLCRestart();

            if (this->high_key < key) this->high_key = key;
            tree->write_node(this);
            this->write_unlock();
            throw OLCRestart();
        }

        auto it = std::upper_bound(keys.begin(), keys.begin() + this->size, key,
                                   this->kcmp);
        if (this->read_unlock_or_restart(version))
            throw OLCRestart(); /* make sure current node is still valid */

        int child_idx = it - keys.begin();
        auto child = get_child(child_idx, false, version);
        auto new_child = child->insert(key, val, split_key, version);

        if (!new_child)
            return nullptr; /* child did not split so the lock is already
                               released in child insert */

        /* insert the key pushed up by the child to current node
         * we may assume that current node will not overflow at this point
         */
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

        /* current lock is upgraded during child insert, release the lock
         * now and restart */
        this->write_unlock();
        throw OLCRestart();

        return nullptr;
    }

    virtual void print(const std::string& padding = "")
    {
        uint64_t version;
        LOG(INFO) << padding << this->high_key;
        this->get_child(0, true, version)->print(padding + "    ");
        for (int i = 0; i < this->size; i++) {
            LOG(INFO) << padding << keys[i];
            this->get_child(i + 1, true, version)->print(padding + "    ");
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
    friend class BTree<N, K, V, KeyComparator, KeyEq>::iterator;

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
        *reinterpret_cast<K*>(buf) = this->high_key;
        buf += sizeof(K);
        ::memcpy(buf, keys.begin(), sizeof(K) * (N - 1));
        buf += sizeof(K) * (N - 1);
        ::memcpy(buf, values.begin(), sizeof(V) * (N - 1));
        buf += sizeof(V) * (N - 1);
    }
    virtual void deserialize(const uint8_t* buf, size_t size)
    {
        this->size = (size_t) * reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);
        this->high_key = *reinterpret_cast<const K*>(buf);
        buf += sizeof(K);
        ::memcpy(keys.begin(), buf, sizeof(K) * (N - 1));
        buf += sizeof(K) * (N - 1);
        ::memcpy(values.begin(), buf, sizeof(V) * (N - 1));
        buf += sizeof(V) * (N - 1);
    }

    virtual void get_values(const K& key, bool upper_bound, bool collect,
                            std::vector<K>* key_list,
                            std::vector<V>& value_list, uint64_t parent_version)
    {
        bool need_restart;
        auto version = this->read_lock_or_restart(need_restart);
        if (need_restart) throw OLCRestart();

        if (this->parent &&
            this->parent->read_unlock_or_restart(parent_version)) {
            throw OLCRestart();
        }

        if (collect) {
            std::copy(keys.begin(), keys.begin() + this->size,
                      std::back_inserter(*key_list));
            std::copy(values.begin(), values.begin() + this->size,
                      std::back_inserter(value_list));
        } else {
            auto lower = std::lower_bound(
                keys.begin(), keys.begin() + this->size, key, this->kcmp);

            if (lower == keys.begin() + this->size) return;

            auto upper = lower;
            while (this->keq(key, *upper))
                upper++;

            std::copy(&values[lower - keys.begin()],
                      &values[upper - keys.begin()],
                      std::back_inserter(value_list));
        }

        if (this->read_unlock_or_restart(version)) throw OLCRestart();
    }

    virtual std::unique_ptr<BaseNode<K, V, KeyComparator, KeyEq>>
    insert(const K& key, const V& val, K& split_key, uint64_t parent_version)
    {
        bool need_restart;
        auto version = this->read_lock_or_restart(need_restart);
        if (need_restart) throw OLCRestart();

        if (this->size == N - 1) { /* leaf node is full, do eager split */
            /* upgrade parent's and own lock to write lock */
            if (this->parent) {
                parent_version = this->parent->upgrade_to_write_lock_or_restart(
                    parent_version, need_restart);
                if (need_restart) throw OLCRestart();
            }

            version =
                this->upgrade_to_write_lock_or_restart(version, need_restart);
            if (need_restart) {
                if (this->parent) {
                    this->parent->write_unlock();
                }
                throw OLCRestart();
            }

            auto right_sibling = tree->template create_node<
                LeafNode<N, K, V, KeyComparator, KeyEq>>(this->parent);

            right_sibling->size = this->size - N / 2;

            ::memcpy(right_sibling->keys.begin(), &this->keys[N / 2],
                     right_sibling->size * sizeof(K));
            ::memcpy(right_sibling->values.begin(), &this->values[N / 2],
                     right_sibling->size * sizeof(V));

            split_key = this->keys[N / 2];
            this->size = N / 2;

            right_sibling->high_key = this->high_key;
            this->high_key = this->keys[this->size - 1];

            tree->write_node(this);
            tree->write_node(right_sibling.get());

            if (this->parent) {
                this->write_unlock();
            }

            /* hold parent's lock */
            return right_sibling;
        }

        /* no need to split, only lock current node */
        version = this->upgrade_to_write_lock_or_restart(version, need_restart);
        if (need_restart) throw OLCRestart();
        if (this->parent) {
            if (this->parent->read_unlock_or_restart(parent_version)) {
                this->write_unlock();
                throw OLCRestart();
            }
        }

        /* we may assume current will not overflow at this point */
        auto it = std::upper_bound(keys.begin(), keys.begin() + this->size, key,
                                   this->kcmp);
        size_t pos = it - keys.begin();

        ::memmove(it + 1, it, (this->size - pos) * sizeof(K));
        ::memmove(&values[pos + 1], &values[pos],
                  (this->size - pos) * sizeof(V));

        keys[pos] = key;
        values[pos] = val;
        this->size++;
        this->high_key = keys[this->size - 1];

        tree->write_node(this);
        this->write_unlock();

        return nullptr;
    }

    virtual void print(const std::string& padding = "")
    {
        LOG(INFO) << padding << "Page ID: " << this->get_pid();
        LOG(INFO) << padding << "High key: " << this->high_key;

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
