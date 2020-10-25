#ifndef __FRAME_HPP__
#define __FRAME_HPP__

#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>

#include "page.hpp"

constexpr auto INVALID_BUFFER_INDEX = -1;

// frame
struct frame_t : public page_t
{
    pagenum_t pagenum;
    int file_id;
    int pin;
    int next;
    int prev;
    bool is_dirty;

    void retain()
    {
        ++pin;
    }

    void release()
    {
        --pin;
    }

    bool valid() const
    {
        return pagenum != EMPTY_PAGE_NUMBER;
    }
    
    bool is_use_now() const
    {
        return pin != 0;
    }

    void init()
    {
        pagenum = EMPTY_PAGE_NUMBER;
        file_id = 0;
        pin = 0;
        next = INVALID_BUFFER_INDEX;
        prev = INVALID_BUFFER_INDEX;
        is_dirty = false;
    }
};

#endif /* __FRAME_HPP__*/