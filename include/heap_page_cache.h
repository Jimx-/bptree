#ifndef _HEAP_PAGE_CACHE_H_
#define _HEAP_PAGE_CACHE_H_

#include "heap_file.h"
#include "page_cache.h"

#include <atomic>
#include <cstdint>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace bptree {

class HeapPageCache : public AbstractPageCache {
public:
    HeapPageCache(HeapFile* file, size_t max_pages);

    virtual Page* new_page();
    virtual Page* fetch_page(PageID id);

    virtual void pin_page(Page* page);
    virtual void unpin_page(Page* page, bool dirty);

    virtual void flush_page(Page* page);
    virtual void flush_all_pages();

    virtual size_t size() const { return pages.size(); }
    virtual size_t get_page_size() const { return page_size; }

private:
    HeapFile* heap_file;
    size_t page_size;
    size_t max_pages;
    std::mutex mutex;
    std::mutex lru_mutex;

    std::list<std::unique_ptr<Page>> pages;
    std::unordered_map<PageID, Page*> page_map;
    std::deque<PageID> lru_list;

    Page* alloc_page(PageID new_id);
    void add_page(Page* page);

    void insert_lru(PageID id);
    void erase_lru(PageID id);
    bool victim(PageID& id);
};

} // namespace bptree

#endif
