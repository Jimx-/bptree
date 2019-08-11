#include "bptree/heap_file.h"

#include <cstdlib>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace bptree {

HeapFile::HeapFile(std::string_view filename, bool create, size_t page_size)
    : filename(filename), page_size(page_size)
{
    fd = -1;

    open(create);
}

HeapFile::~HeapFile()
{
    if (is_open()) {
        close();
    }
}

PageID HeapFile::new_page()
{
    std::lock_guard<std::mutex> lock(mutex);

    PageID new_page = (PageID)file_size_pages;
    ftruncate(fd, file_size_pages * page_size);

    file_size_pages++;
    write_header();

    return new_page;
}

void HeapFile::read_page(Page* page, boost::upgrade_to_unique_lock<Page>& lock)
{
    auto pid = page->get_id();
    if (pid == Page::INVALID_PAGE_ID) {
        throw IOException("page ID is invalid");
    }

    if (pid >= file_size_pages) {
        throw IOException("page ID >= # pages");
    }

    auto* buf = page->get_buffer(lock);
    lseek(fd, pid * page_size, SEEK_SET);
    read(fd, buf, page_size);
}

void HeapFile::write_page(Page* page, boost::upgrade_lock<Page>& lock)
{
    auto pid = page->get_id();
    if (pid == Page::INVALID_PAGE_ID) {
        throw IOException("page ID is invalid");
    }

    if (pid >= file_size_pages) {
        throw IOException("page ID >= # pages");
    }

    const auto* buf = page->get_buffer(lock);
    lseek(fd, pid * page_size, SEEK_SET);
    write(fd, buf, page_size);
}

void HeapFile::open(bool create)
{
    struct stat sbuf;
    int err = ::stat(filename.c_str(), &sbuf);

    if (err < 0 && errno == ENOENT) {
        if (create) {
            this->create();
            return;
        }
    }

    if (err < 0) {
        fd = -1;
        throw IOException("unable to get heap file status");
    }

    fd = ::open(filename.c_str(), O_RDWR);
    if (fd < 0) {
        fd = -1;
        throw IOException("unable to open heap file");
    }

    read_header();
}

void HeapFile::close()
{
    ::close(fd);
    fd = -1;
}

void HeapFile::create()
{
    fd = ::open(filename.c_str(), O_RDWR | O_CREAT | O_EXCL,
                S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd < 0) {
        fd = -1;
        throw IOException("unable to create heap file");
    }

    int err = ftruncate(fd, page_size);
    file_size_pages = 1;
    if (err != 0) {
        fd = -1;
        throw IOException("unable to resize heap file");
    }

    write_header();
}

void HeapFile::read_header()
{
    uint32_t magic;

    lseek(fd, 0, SEEK_SET);
    read(fd, &magic, sizeof(magic));
    if (magic != MAGIC) {
        throw IOException("bad heap file(magic)");
    }

    read(fd, &page_size, sizeof(page_size));
    read(fd, &file_size_pages, sizeof(file_size_pages));
}

void HeapFile::write_header()
{
    uint32_t magic = MAGIC;

    lseek(fd, 0, SEEK_SET);
    write(fd, &magic, sizeof(magic));
    write(fd, &page_size, sizeof(page_size));
    write(fd, &file_size_pages, sizeof(file_size_pages));
}

} // namespace bptree
