#ifndef _MEM_PAGE_CACHE_H_
#define _MEM_PAGE_CACHE_H_

#include "bptree/page_cache.h"

#include <shared_mutex>
#include <unordered_map>

namespace bptree {

#define PAGE_SIZE 4096

class MemPageCache : public AbstractPageCache {
public:
    MemPageCache(size_t page_size) : page_size(page_size) { next_id.store(1); }

    virtual Page* new_page()
    {
        auto id = get_next_id();
        std::unique_lock<std::shared_mutex> lock(mutex);
        page_map[id] = std::make_unique<Page>(id, page_size);
        return page_map[id].get();
    }

    virtual Page* fetch_page(PageID id)
    {
        std::shared_lock<std::shared_mutex> lock(mutex);
        auto it = page_map.find(id);
        if (it == page_map.end()) return nullptr;
        return it->second.get();
    }

    virtual void pin_page(Page* page) {}
    virtual void unpin_page(Page* page, bool dirty) {}

    virtual void flush_page(Page* page) {}
    virtual void flush_all_pages() {}

    virtual size_t size() const { return page_map.size(); }
    virtual size_t get_page_size() const { return page_size; }

private:
    size_t page_size;
    std::atomic<PageID> next_id;
    std::shared_mutex mutex;
    std::unordered_map<PageID, std::unique_ptr<Page>> page_map;

    PageID get_next_id() { return next_id++; }
};

} // namespace bptree

#endif
