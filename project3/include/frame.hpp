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
    int table_id;
    pagenum_t pagenum;
    bool is_dirty;
    bool is_pinned;
};

#endif /* __FRAME_HPP__*/