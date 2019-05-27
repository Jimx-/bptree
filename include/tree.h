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
                root_node->get_value(key, value_list, 0);
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

private:
    static const PageID META_PAGE_ID = 1;
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
