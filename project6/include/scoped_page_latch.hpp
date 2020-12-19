#ifndef __SCOPTED_PAGE_LATCH_HPP__
#define __SCOPTED_PAGE_LATCH_HPP__

#include "page.hpp"

class scoped_node_latch
{
 public:
    scoped_node_latch(int manager_id, pagenum_t id, int buffer_index);
    ~scoped_node_latch();
    void lock();
    void unlock();

 private:
    int manager_id;
    pagenum_t id;
    int buffer_index;
    std::atomic<bool> locked;
};

class scoped_node_latch_shared
{
 public:
    scoped_node_latch_shared(int manager_id, pagenum_t id, int buffer_index);
    ~scoped_node_latch_shared();
    void lock_shared();
    void unlock_shared();

 private:
    int manager_id;
    pagenum_t id;
    int buffer_index;
    std::atomic<bool> locked;
};

#endif /* __SCOPTED_PAGE_LATCH_HPP__*/