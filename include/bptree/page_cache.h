#ifndef _BPTREE_PAGE_CACHE_H_
#define _BPTREE_PAGE_CACHE_H_

#include "bptree/page.h"

namespace bptree {

class AbstractPageCache {
public:
    virtual Page* new_page() = 0;
    virtual Page* fetch_page(PageID id) = 0;

    virtual void pin_page(Page* page) = 0;
    virtual void unpin_page(Page* page, bool dirty) = 0;

    virtual void flush_page(Page* page) = 0;
    virtual void flush_all_pages() = 0;

    virtual size_t size() const = 0;
    virtual size_t get_page_size() const = 0;
};

} // namespace bptree

#endif
