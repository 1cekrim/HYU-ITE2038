#include "lock_manager.hpp"

#include <algorithm>
#include <thread>

lock_t* LockManager::lock_acquire(int table_id, int64_t key)
{
    auto lock = std::make_shared<lock_t>(table_id, key);
    if (!lock)
    {
        return nullptr;
    }

    {
        std::unique_lock<std::mutex> crit { mtx };
        lock->locked = !lock_table[table_id][key].empty();
        lock_table[table_id][key].emplace_back(lock);
    }

    while (lock->wait())
    {
        std::this_thread::yield();
    }

    return lock.get();
}

int LockManager::lock_release(lock_t* lock_obj)
{
    {
        std::unique_lock<std::mutex> crit { mtx };
        int table_id = lock_obj->table_id;
        int64_t key = lock_obj->key;
        auto& table = lock_table[table_id][key];

        auto iter =
            std::find_if(table.begin(), table.end(), [lock_obj](auto& p) {
                return p.get() == lock_obj;
            });

        iter = table.erase(iter);

        if (!table.empty() && iter == table.begin())
        {
            iter->get()->signal();
        }
    }

    return 0;
}

lock_t::lock_t(int table_id, int64_t key)
    : table_id(table_id),
      key(key),
      locked(false)
{
    // Do nothing
}

bool lock_t::wait() const
{
    return locked;
}

void lock_t::signal()
{
    locked = false;
}