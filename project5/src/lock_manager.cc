#include "lock_manager.hpp"

#include <algorithm>
#include <thread>

#include "logger.hpp"
#include "transaction_manager.hpp"

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
    auto lock = std::make_shared<lock_t>(table_id, key, mode, trx_id);
    LockHash hash { table_id, key };
    if (!lock)
    {
        return nullptr;
    }

    {
        std::unique_lock<std::mutex> crit { mtx };
        if (auto it = lock_table.find(hash);
            it == lock_table.end() || (it->second.mode == LockMode::EMPTY ||
                                       (it->second.mode == LockMode::SHARED &&
                                        lock->lockMode == LockMode::SHARED)))
        {
            // 바로 실행
            if (it == lock_table.end())
            {
                lock_table.emplace(hash, LockList());
                it = lock_table.find(hash);
            }
            lock->state = LockState::ACQUIRED;
            lock->signal();
            lock->locked = false;
            it->second.mode = mode;
            it->second.locks.push_front(lock);
            ++it->second.acquire_count;
            return lock;
        }

        // TODO: Transaction 관련 추가

        lock->state = LockState::WAITING;
        lock->locked = true;
        lock_table[hash].locks.emplace_back(lock);
        ++lock_table[hash].wait_count;
    }

    while (lock->wait())
    {
        std::this_thread::yield();
    }

    return lock;
}

bool LockManager::lock_release(std::shared_ptr<lock_t> lock_obj)
{
    {
        std::unique_lock<std::mutex> crit { mtx };
        int table_id = lock_obj->table_id;
        int64_t key = lock_obj->key;
        LockHash hash { table_id, key };
        auto& lockList = lock_table[hash];
        auto& table = lockList.locks;

        auto iter =
            std::find_if(table.begin(), table.end(), [lock_obj](auto& p) {
                return p.get() == lock_obj.get();
            });

        CHECK(iter != table.end());

        if (iter->get()->state == LockState::ACQUIRED)
        {
            --lockList.acquire_count;
        }
        else
        {
            // wait 중인걸 그냥 lock_release 해도 되나?
            exit(-1);
            --lockList.wait_count;
        }
        table.erase(iter);

        if (lockList.acquire_count > 0)
        {
            return true;
        }

        if (lockList.wait_count == 0)
        {
            lock_table.erase(hash);
            return true;
        }

        if (auto& target = table.front();
            target->lockMode == LockMode::EXCLUSIVE)
        {
            CHECK(target->state == LockState::WAITING);
            lockList.mode = LockMode::EXCLUSIVE;
            target->state = LockState::ACQUIRED;
            target->signal();
            return true;
        }

        lockList.mode = LockMode::SHARED;

        for (auto& it : lockList.locks)
        {
            if (it->lockMode == LockMode::EXCLUSIVE)
            {
                break;
            }
            it->state = LockState::ACQUIRED;
            it->signal();
        }
    }

    return true;
}

const std::map<LockHash, LockList>& LockManager::get_table() const
{
    return lock_table;
}

void LockManager::reset()
{
    std::unique_lock<std::mutex> lock(mtx);
    lock_table.clear();
}

lock_t::lock_t(int table_id, int64_t key, LockMode lockMode,
               int ownerTransactionID)
    : table_id(table_id),
      key(key),
      locked(false),
      lockMode(lockMode),
      ownerTransactionID(ownerTransactionID)
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

    // TODO: state 관련도 정리할 필요가 있음

    auto& transaction = TransactionManager::instance().get(ownerTransactionID);
    transaction.state = TransactionState::RUNNING;
}

LockList::LockList() : wait_count(0), acquire_count(0)
{
    // Do nothing
}

LockList::LockList(const LockList& rhs)
    : mode(rhs.mode.load()),
      wait_count(rhs.wait_count.load()),
      acquire_count(rhs.acquire_count.load())
{
    // Do nothing
}