#include "lock_manager.hpp"

#include <algorithm>
#include <iostream>
#include <set>
#include <thread>
#include <vector>

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
    LockHash hash{ table_id, key };
    if (!lock)
    {
        return nullptr;
    }

    {
        std::unique_lock<std::mutex> crit{ mtx };
        if (auto it = lock_table.find(hash);
            it == lock_table.end() || (it->second.mode == LockMode::SHARED &&
                                       lock->lockMode == LockMode::SHARED))
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
        auto& table = lock_table[hash];
        if (table.mode == LockMode::SHARED && mode == LockMode::EXCLUSIVE)
        {
            table.mode = LockMode::EXCLUSIVE;
        }
        table.locks.emplace_back(lock);
        ++table.wait_count;
    }

    CHECK_RET(deadlock_detection(), nullptr);

    while (lock->wait())
    {
        std::this_thread::yield();
    }

    return lock;
}

bool LockManager::deadlock_detection()
{
    std::unique_lock<std::mutex> crit{ mtx };

    std::unordered_map<int, std::tuple<std::set<int>, std::set<int>>> graph;

    for (const auto& lock : lock_table)
    {
        const auto& list = lock.second;
        const auto wait_begin =
            std::next(list.locks.begin(), list.acquire_count);
        for (auto acquire = list.locks.begin(); acquire != wait_begin;
             ++acquire)
        {
            auto acquire_id = acquire->get()->ownerTransactionID;
            for (auto wait = wait_begin; wait != list.locks.end(); ++wait)
            {
                auto wait_id = wait->get()->ownerTransactionID;
                next_graph[acquire_id].insert(wait_id);
                before_graph[wait_id].insert(acquire_id);
            }
        }
    }

    if constexpr (true)
    {
        std::cout << "\ndeadlock detection next\n";

        for (const auto& node : next_graph)
        {
            std::cout << "node " << node.first << ':';
            for (const auto& next : node.second)
            {
                std::cout << next << ' ';
            }
            std::cout << '\n';
        }

        std::cout << "before\n";
        for (const auto& node : before_graph)
        {
            std::cout << "node " << node.first << ':';
            for (const auto& before : node.second)
            {
                std::cout << before << ' ';
            }
            std::cout << '\n';
        }
    }

    return true;
}

bool LockManager::lock_release(std::shared_ptr<lock_t> lock_obj)
{
    {
        std::unique_lock<std::mutex> crit{ mtx };
        int table_id = lock_obj->table_id;
        int64_t key = lock_obj->key;
        LockHash hash{ table_id, key };
        auto& lockList = lock_table[hash];
        auto& table = lockList.locks;

        auto iter = std::find_if(
            table.begin(), table.end(),
            [lock_obj](auto& p) { return p.get() == lock_obj.get(); });

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
            // TODO: state, count 바꾸는 거 signal에서 담당하게
            target->state = LockState::ACQUIRED;
            ++lockList.acquire_count;
            --lockList.wait_count;
            target->signal();
            return true;
        }

        lockList.mode = LockMode::SHARED;

        for (auto& it : lockList.locks)
        {
            if (it->lockMode == LockMode::EXCLUSIVE)
            {
                lockList.mode = LockMode::EXCLUSIVE;
                break;
            }
            it->state = LockState::ACQUIRED;
            ++lockList.acquire_count;
            --lockList.wait_count;
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