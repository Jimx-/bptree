#ifndef _BPTREE_HEAP_FILE_H_
#define _BPTREE_HEAP_FILE_H_

#include "bptree/page.h"

#include <mutex>
#include <stdexcept>
#include <string>

namespace bptree {

class IOException : public std::runtime_error {
public:
    IOException(const char* message) : runtime_error(message) {}
};

class HeapFile {
public:
    explicit HeapFile(const std::string& filename, bool create,
                      size_t page_size);
    ~HeapFile();

    bool is_open() const { return fd != -1; }
    size_t get_page_size() const { return page_size; }

    PageID new_page();
    void read_page(Page* page);
    void write_page(Page* page);

private:
    static const uint32_t MAGIC = 0xDEADBEEF;

    int fd;
    size_t page_size;
    uint32_t file_size_pages;
    std::string filename;
    std::mutex mutex;

    void create();
    void open(bool create);
    void close();

    void read_header();
    void write_header();
};

} // namespace bptree

#endif
