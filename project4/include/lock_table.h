#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>

#include <algorithm>
#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <iostream>

struct lock_t
{
    int table_id;
    int64_t key;
    std::atomic<bool> locked;

    lock_t(int table_id, int64_t key);

    bool wait() const;
    void signal();
};

class LockManager
{
 public:
    lock_t* lock_acquire(int table_id, int64_t key);
    int lock_release(lock_t* lock_obj);

 private:
    std::mutex mtx;
    std::unordered_map<
        int, std::unordered_map<int64_t, std::list<std::shared_ptr<lock_t>>>>
        lock_table;
};

/* APIs for lock table */
int init_lock_table();
lock_t* lock_acquire(int table_id, int64_t key);
int lock_release(lock_t* lock_obj);

#endif /* __LOCK_TABLE_H__ */
