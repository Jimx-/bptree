#include "heap_page_cache.h"

#include "easylogging++.h"

#include <cassert>

namespace bptree {

HeapPageCache::HeapPageCache(HeapFile* file, size_t max_pages)
    : heap_file(file), max_pages(max_pages)
{
    page_size = file->get_page_size();
}

Page* HeapPageCache::alloc_page(PageID id)
{
    std::lock_guard<std::mutex> lock(mutex);

    if (size() < max_pages) {
        auto page = new Page(id, page_size);
        pages.emplace_back(page);
        page_map[id] = page;

        return page;
    }

    PageID victim_id;
    if (!victim(victim_id)) {
        return nullptr;
    }

    auto it = page_map.find(victim_id);
    assert(it != page_map.end());

    auto* page = it->second;

    page_map.erase(it);

    if (page->is_dirty()) {
        flush_page(page);
    }
    page->set_id(id);
    page_map[id] = page;

    return page;
}

Page* HeapPageCache::new_page()
{
    PageID new_id = heap_file->new_page();
    auto page = alloc_page(new_id);

    pin_page(page);

    return page;
}

Page* HeapPageCache::fetch_page(PageID id)
{
    auto it = page_map.find(id);

    if (it == page_map.end()) {
        auto page = alloc_page(id);

        try {
            heap_file->read_page(page);
            pin_page(page);

            return page;
        } catch (const IOException&) {
            return nullptr;
        }
    }

    pin_page(it->second);
    return it->second;
}

void HeapPageCache::pin_page(Page* page)
{
    page->lock();

    if (!page->get_pin_count()) {
        erase_lru(page->get_id());
    }
    page->pin();

    page->unlock();
}

void HeapPageCache::unpin_page(Page* page, bool dirty)
{
    page->lock();

    if (!page->get_pin_count()) {
        page->unlock();
        throw IOException("try to unpin page with pin_count == 0");
    }

    page->set_dirty(dirty);
    page->unpin();

    if (!page->get_pin_count()) {
        insert_lru(page->get_id());
    }

    page->unlock();

    flush_page(page);
}

void HeapPageCache::flush_page(Page* page)
{
    if (page->is_dirty()) {
        heap_file->write_page(page);

        page->set_dirty(false);
    }
}

void HeapPageCache::flush_all_pages()
{
    for (auto&& p : pages) {
        flush_page(p.get());
    }
}

void HeapPageCache::add_page(Page* page)
{
    std::lock_guard<std::mutex> lock(mutex);

    page_map[page->get_id()] = page;
}

void HeapPageCache::insert_lru(PageID id)
{
    std::lock_guard<std::mutex> lock(lru_mutex);
    lru_list.push_front(id);
}

void HeapPageCache::erase_lru(PageID id)
{
    std::lock_guard<std::mutex> lock(lru_mutex);

    auto it = std::find(lru_list.begin(), lru_list.end(), id);

    if (it != lru_list.end()) {
        lru_list.erase(it);
    }
}

bool HeapPageCache::victim(PageID& id)
{
    std::lock_guard<std::mutex> lock(lru_mutex);

    if (lru_list.empty()) {
        return false;
    }

    id = lru_list.back();
    lru_list.pop_back();
    return true;
}

} // namespace bptree
