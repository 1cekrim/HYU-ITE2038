#include "lock_manager.hpp"

#include <algorithm>
#include <thread>

LockHash::LockHash() : LockHash(invalid_table_id, invalid_key)
{
    // Do nothing
}

LockHash::LockHash(int table_id, int64_t key) : table_id(table_id), key(key)
{
    // Do nothing
}

bool LockHash::operator<(const LockHash& rhs) const
{
    return table_id < rhs.table_id ||
           (table_id == rhs.table_id && key < rhs.key);
}
bool LockHash::operator==(const LockHash& rhs) const
{
    return table_id == rhs.table_id && key == rhs.key;
}

std::shared_ptr<lock_t> LockManager::lock_acquire(int table_id, int64_t key,
                                                  int trx_id, LockMode mode)
{
    auto lock = std::make_shared<lock_t>(table_id, key);
    LockHash hash { table_id, key };
    if (!lock)
    {
        return nullptr;
    }

    {
        std::unique_lock<std::mutex> crit { mtx };
        lock->locked = !lock_table[hash].locks.empty();
        lock_table[hash].locks.emplace_back(lock);
    }

    while (lock->wait())
    {
        std::this_thread::yield();
    }

    return lock;
}

int LockManager::lock_release(std::shared_ptr<lock_t> lock_obj)
{
    {
        std::unique_lock<std::mutex> crit { mtx };
        int table_id = lock_obj->table_id;
        int64_t key = lock_obj->key;
        LockHash hash { table_id, key };
        auto& table = lock_table[hash].locks;

        auto iter =
            std::find_if(table.begin(), table.end(), [lock_obj](auto& p) {
                return p.get() == lock_obj.get();
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