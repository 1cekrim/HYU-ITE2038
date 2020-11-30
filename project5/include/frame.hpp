#ifndef __FRAME_HPP__
#define __FRAME_HPP__

#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>

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
    std::recursive_mutex mtx;

    void print_frame()
    {
        print_node();
        std::cout << "pagenum: " << pagenum << "\nfile_id: " << file_id
                  << "\npin: " << pin << "\nnext: " << next
                  << "\nprev: " << prev << '\n';
    }

    void copy_without_mtx(frame_t& rhs)
    {
        rhs.pagenum = pagenum;
        rhs.file_id = file_id;
        rhs.pin = pin;
        rhs.next = next;
        rhs.prev = prev;
        rhs.is_dirty = is_dirty;
        rhs.change_page(*this);
    }

    void change_page(const frame_t& frame)
    {
        this->header = frame.header;
        this->entry = frame.entry;
    }

    void retain()
    {
        mtx.lock();
        if (pin < 0)
        {
            std::cout << "retain: " << pagenum << ", " << pin << std::endl;
        }
        ++pin;
    }

    void release()
    {
        mtx.unlock();
        --pin;
        if (pin < 0)
        {
        std::cout << "release: " << pagenum << std::endl;
            std::cout << "pin: " << pin << std::endl;
            std::cout << "pin: " << pin << std::endl;
        }
    }

    bool valid() const
    {
        return file_id != -1;
    }

    bool is_use_now() const
    {
        return pin != 0;
    }

    void init()
    {
        pagenum = EMPTY_PAGE_NUMBER;
        file_id = -1;
        pin = 0;
        next = INVALID_BUFFER_INDEX;
        prev = INVALID_BUFFER_INDEX;
        is_dirty = false;
    }
};

#endif /* __FRAME_HPP__*/