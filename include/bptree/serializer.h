#ifndef _BPTREE_SERIALIZER_H_
#define _BPTREE_SERIALIZER_H_

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace bptree {

template <typename T> class AbstractSerializer {
public:
    /* serialize elements between begin and end and write them to buf. returns
     * number of bytes used */
    virtual size_t serialize(uint8_t* buf, size_t buf_size, const T* begin,
                             const T* end) const = 0;
    /* deserialize elements in buf and write to elements between begin and end.
     * returns number of bytes consumed */
    virtual size_t deserialize(T* begin, T* end, const uint8_t* buf,
                               size_t buf_size) const = 0;
};

template <typename T> class CopySerializer {
public:
    virtual size_t serialize(uint8_t* buf, size_t buf_size, const T* begin,
                             const T* end) const
    {
        size_t bytes_used = (end - begin) * sizeof(T);
        ::memcpy(buf, begin, bytes_used);
        return bytes_used;
    }

    virtual size_t deserialize(T* begin, T* end, const uint8_t* buf,
                               size_t buf_size) const
    {
        size_t bytes_consumed = (end - begin) * sizeof(T);
        ::memcpy(begin, buf, bytes_consumed);
        return bytes_consumed;
    }
};

} // namespace bptree

#endif
