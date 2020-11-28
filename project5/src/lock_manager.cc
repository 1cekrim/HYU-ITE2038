#include "lock_manager.hpp"

#include <algorithm>
#include <iostream>
#include <queue>
#include <set>
#include <thread>
#include <vector>

#include "logger.hpp"
#include "transaction_manager.hpp"

LockHash::LockHash() : LockHash(invalid_table_id, invalid_key, invalid_key)
{
    // Do nothing
}

LockHash::LockHash(int table_id, int key, int record_index)
    : table_id(table_id), key(key), record_index(record_index)
{
    // Do nothing
}

bool LockHash::operator<(const LockHash& rhs) const
{
    return table_id < rhs.table_id ||
           (table_id == rhs.table_id && key < rhs.key) ||
           (table_id == rhs.table_id && key == rhs.key &&
            record_index < rhs.record_index);
}
bool LockHash::operator==(const LockHash& rhs) const
{
    return table_id == rhs.table_id && key == rhs.key &&
           record_index == rhs.record_index;
}

std::shared_ptr<lock_t> LockManager::lock_acquire(int table_id, int key, int record_index,
                                                  int trx_id, LockMode mode)
{
    auto lock = std::make_shared<lock_t>(LockHash(table_id, key, record_index), mode, trx_id);
    LockHash hash{ table_id, key, record_index};
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

    if (deadlock_detection(trx_id))
    {
        TransactionManager::instance().abort(trx_id);
        return nullptr;
    }

    while (lock->wait())
    {
        std::this_thread::yield();
    }

    return lock;
}

bool LockManager::deadlock_detection(int now_transaction_id)
{
    std::unique_lock<std::mutex> crit{ mtx };

    std::unordered_map<int, std::set<int>> graph;

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
                graph[acquire_id].insert(wait_id);
            }
        }
    }

    std::queue<int> q;
    q.push(now_transaction_id);
    std::vector<int> visited;
    visited.push_back(now_transaction_id);

    while (!q.empty())
    {
        int now = q.front();
        q.pop();

        for (const auto next : graph[now])
        {
            if (std::find(visited.begin(), visited.end(), next) !=
                visited.end())
            {
                // Deadlock detected!
                return true;
            }
            q.push(now);
            visited.push_back(next);
        }
    }

    if constexpr (false)
    {
        std::cout << "\ndeadlock detection next\n";

        for (const auto& node : graph)
        {
            std::cout << "node " << node.first << ':';
            for (const auto& next : node.second)
            {
                std::cout << next << ' ';
            }
            std::cout << '\n';
        }
    }

    return false;

    // std::unordered_map<int, std::tuple<std::set<int>, std::set<int>>> graph;

    // constexpr auto next_graph = 0;
    // constexpr auto prev_graph = 1;

    // for (const auto& lock : lock_table)
    // {
    //     const auto& list = lock.second;
    //     const auto wait_begin =
    //         std::next(list.locks.begin(), list.acquire_count);
    //     for (auto acquire = list.locks.begin(); acquire != wait_begin;
    //          ++acquire)
    //     {
    //         auto acquire_id = acquire->get()->ownerTransactionID;
    //         for (auto wait = wait_begin; wait != list.locks.end(); ++wait)
    //         {
    //             auto wait_id = wait->get()->ownerTransactionID;
    //             std::get<next_graph>(graph[acquire_id]).insert(wait_id);
    //             std::get<prev_graph>(graph[wait_id]).insert(acquire_id);
    //         }
    //     }
    // }

    // for (const auto& node : graph)
    // {
    //     const auto now_id = node.first;
    //     const auto& next = std::get<next_graph>(node.second);
    //     const auto& prev = std::get<prev_graph>(node.second);

    // }

    // if constexpr (true)
    // {
    //     std::cout << "\ndeadlock detection\n";

    //     for (const auto& node : graph)
    //     {
    //         std::cout << "node " << node.first << '\n';
    //         std::cout << "next: ";
    //         for (const auto& next : std::get<next_graph>(node.second))
    //         {
    //             std::cout << next << ' ';
    //         }
    //         std::cout << "\nbefore: ";
    //         for (const auto& before : std::get<prev_graph>(node.second))
    //         {
    //             std::cout << before << ' ';
    //         }
    //         std::cout << '\n';
    //     }
    // }
}

bool LockManager::lock_release(std::shared_ptr<lock_t> lock_obj)
{
    {
        std::unique_lock<std::mutex> crit{ mtx };
        LockHash hash = lock_obj->hash;

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

lock_t::lock_t(LockHash hash, LockMode lockMode,
               int ownerTransactionID)
    : hash(hash),
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