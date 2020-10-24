#ifndef __FRAME_HPP__
#define __FRAME_HPP__

#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>

#include "page.hpp"

// frame
struct frame_t : public page_t
{
    pagenum_t pagenum;
    int file_id;
    int pin;
    int lru_next;
    bool is_dirty;
};

#endif /* __FRAME_HPP__*/