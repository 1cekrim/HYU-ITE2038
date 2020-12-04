#include "lock_manager.hpp"

#include <algorithm>
#include <iostream>
#include <queue>
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

std::ostream& operator<<(std::ostream& os, const LockMode& dt)
{
    switch (dt)
    {
        case LockMode::EMPTY:
            os << "EMPTY";
            break;
        case LockMode::EXCLUSIVE:
            os << "EXCLUSIVE";
            break;
        case LockMode::SHARED:
            os << "SHARED";
            break;
    }
    return os;
}

std::shared_ptr<lock_t> LockManager::lock_acquire(int table_id, int64_t key,
                                                  int trx_id, LockMode mode)
{
    std::shared_ptr<lock_t> lock;

    {
        std::unique_lock<std::mutex> crit { mtx };
        auto num = lock_table.size();
        lock = std::make_shared<lock_t>(LockHash(table_id, key), mode, trx_id);
        LockHash hash { table_id, key };
        if (!lock)
        {
            return nullptr;
        }
        num += 0;
        // if (key == 37 && trx_id == 158)
        // {
        //     std::cout << "special test\n";
        //     for (const auto& lock : lock_table)
        //     {
        //         const auto& list = lock.second;
        //         std::cout << "lock:" << lock.first.table_id << " "
        //                   << lock.first.key << '\n';
        //         list.print();
        //     }
        //     auto it = lock_table.find(hash);
        //     if (it == lock_table.end())
        //     {
        //         std::cout << "end??\n";
        //     }
        //     else
        //     {
        //         std::cout << "hmm...\n";
        //         it->second.print();
        //         std::cout << "\nlockMode:" << lock->lockMode << "\n";
        //     }
        // }
        if (auto it = lock_table.find(hash);
            it == lock_table.end() || (it->second.mode == LockMode::SHARED &&
                                       lock->lockMode == LockMode::SHARED))
        {
            // std::cout << "in if\n";
            // // 바로 실행
            // if (it == lock_table.end())
            // {
            //     lock_table.emplace(hash, LockList());
            //     it = lock_table.find(hash);
            // }
            // std::cout << "new lock: " << trx_id << ", table_id: " << table_id
            // << ", key:" << key << "\n";
            auto& list = lock_table[hash];
            // std::cout << hash.key << ' ' << hash.table_id << std::endl;
            lock->state = LockState::ACQUIRED;
            lock->signal();
            lock->locked = false;
            list.mode = mode;
            list.locks.push_front(lock);
            ++list.acquire_count;
            // std::cout << "trx_id: " << trx_id << "table_id: " << table_id
            //           << ", key: " << key << " pass\n";
            return lock;
        }

        // TODO: Transaction 관련 추가
        TransactionManager::instance().get(trx_id).state =
            TransactionState::WAITING;

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
        lock_release(lock);
        // std::cout << "\nafter\n";
        // if (trx_id == 150)
        // {
        //     for (const auto& lock : lock_table)
        //     {
        //         const auto& list = lock.second;
        //         list.print();
        //     }
        // }
        return nullptr;
    }
    // std::cout << "trx_id: " << trx_id << "table_id: " << table_id
    //           << ", key: " << key << " wait\n";
    while (lock->wait())
    {
        std::this_thread::yield();
    }

    return lock;
}

std::shared_ptr<lock_t> LockManager::lock_upgrade(int table_id, int64_t key,
                                                  int trx_id, LockMode mode)
{
    CHECK_RET(mode == LockMode::EXCLUSIVE, nullptr);
    auto lock = std::make_shared<lock_t>(LockHash(table_id, key), mode, trx_id);
    LockHash hash { table_id, key };
    if (!lock)
    {
        return nullptr;
    }

    {
        std::unique_lock<std::mutex> crit { mtx };
        auto locklist_it = lock_table.find(hash);
        /*
        lock_upgrade는, SLock을 획득한 트랜잭션이 XLock을 획득하려 할 때
        호출되는 메소드이다. 따라서 제공된 hash에 해당하는 lock list가 존재하지
        않다면 논리적 오류가 발생한 것이다.
        */
        CHECK_RET(locklist_it != lock_table.end(), nullptr);

        auto& lock_list = locklist_it->second;

        auto slock_it =
            std::find_if(lock_list.locks.begin(), lock_list.locks.end(),
                         [trx_id](const auto& lock) {
                             return lock->ownerTransactionID == trx_id;
                         });

        /*
        제공된 hash에 해당하는 lock list에, trx_id를 owner로 하는 lock이
        존재하지 않다면 논리적 오류가 발생한 것이다.
        */
        CHECK_RET(slock_it != lock_list.locks.end(), nullptr);

        /*
        특수한 경우. lock_list에 trx_id가 owner인 SLock만 존재할 경우
        이 경우 XLock을 바로 획득할 수 있다.
        */
        if (lock_list.wait_count == 0 && lock_list.acquire_count == 1)
        {
            lock->state = LockState::ACQUIRED;
            lock->signal();
            lock->locked = false;
            lock_list.mode = mode;
            lock_list.locks.push_back(lock);
            ++lock_list.acquire_count;
            return lock;
        }
        /*
        일반적인 경우. lock_list에 다른 트랜잭션의 lock이 존재할 경우
        이런 경우 순서상 우선인 트랜잭션들이 모두 commit될 때까지 대기해야 한다.
        */
        TransactionManager::instance().get(trx_id).state =
            TransactionState::WAITING;

        lock->state = LockState::WAITING;
        lock->locked = true;
        lock_list.mode = LockMode::EXCLUSIVE;
        lock_list.locks.emplace_back(lock);
        ++lock_list.wait_count;
    }

    if (deadlock_detection(trx_id))
    {
        TransactionManager::instance().abort(trx_id);
        lock_release(lock);
        return nullptr;
    }

    while (lock->wait())
    {
        std::this_thread::yield();
    }

    return lock;
}

void LockManager::dfs(int now, std::unordered_map<int, graph_node>& graph,
                      bool& stop)
{
    graph[now].visited = true;
    for (const auto& next : graph[now].next)
    {
        if (graph[next].visited)
        {
            stop = true;
            return;
        }
        dfs(next, graph, stop);
        if (stop)
        {
            return;
        }
    }
    graph[now].visited = false;
}

bool LockManager::deadlock_detection(int now_transaction_id)
{
    std::unique_lock<std::mutex> crit { mtx };

    std::unordered_map<int, graph_node> graph;

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
                if (acquire_id == wait_id)
                {
                    continue;
                }
                graph[wait_id].next.insert(acquire_id);
                graph[wait_id].visited = false;
            }
        }
    }

    bool deadlock = false;
    dfs(now_transaction_id, graph, deadlock);

    // if (deadlock && now_transaction_id == 150)
    // {
    //     for (const auto& lock : lock_table)
    //     {
    //         const auto& list = lock.second;
    //         std::cout << "lock:" << lock.first.table_id << " " << lock.first.key
    //                   << '\n';
    //         list.print();
    //     }
    // }

    return deadlock;

    // std::queue<int> q;
    // q.push(now_transaction_id);
    // std::vector<int> visited;

    // while (!q.empty())
    // {
    //     int now = q.front();
    //     q.pop();
    //      if (std::find(visited.begin(), visited.end(), now) !=
    //             visited.end())
    //         {
    //             std::cout << "deadlock!: ";
    //             for (const auto& visit : visited)
    //             {
    //                 std::cout << visit << ' ';
    //             }
    //             std::cout << '\n';
    //             for (const auto& lock : lock_table)
    //             {
    //                 lock.second.print();
    //             }
    //             for (const auto& node : graph)
    //             {
    //                 std::cout << "node " << node.first << ':';
    //                 for (const auto& next : node.second)
    //                 {
    //                     std::cout << next << ' ';
    //                 }
    //                 std::cout << '\n';
    //             }
    //             return true;
    //         }
    //         visited.push_back(now);
    //     for (const auto next : graph[now])
    //     {

    //         q.push(next);
    //     }
    // }

    // if constexpr (false)
    // {
    //     std::cout << "\ndeadlock detection next\n";

    //     for (const auto& node : graph)
    //     {
    //         std::cout << "node " << node.first << ':';
    //         for (const auto& next : node.second)
    //         {
    //             std::cout << next << ' ';
    //         }
    //         std::cout << '\n';
    //     }
    // }

    return false;
}

bool LockManager::lock_release(std::shared_ptr<lock_t> lock_obj)
{
    {
        std::unique_lock<std::mutex> crit { mtx };
        LockHash hash = lock_obj->hash;
        // std::cout << "release start (" << lock_obj->hash.key << ") -> ";
        auto& lockList = lock_table[hash];
        auto& table = lockList.locks;

        auto iter =
            std::find_if(table.begin(), table.end(), [lock_obj](auto& p) {
                return p.get() == lock_obj.get();
            });

        CHECK(iter != table.end());

        if (iter->get()->state == LockState::ACQUIRED)
        {
            // std::cout << "acquire -> ";
            --lockList.acquire_count;
        }
        else
        {
            // wait 중인걸 그냥 lock_release 해도 되나?
            // TODO: abort 될때 처리...
            // std::cout << "wait -> ";
            --lockList.wait_count;
        }
        table.erase(iter);

        if (lockList.acquire_count > 0)
        {
            if (lockList.acquire_count == 1 && lockList.wait_count >= 1)
            {
                auto& first = *lockList.locks.begin();
                auto& second = *std::next(lockList.locks.begin());
                if (first->ownerTransactionID == second->ownerTransactionID)
                {
                    /*
                    하나의 트랜잭션을 owner로 하는 SLock과 XLock이 만났을 때
                    XLock을 깨워준다
                    */
                    CHECK(first->lockMode == LockMode::SHARED);
                    CHECK(second->lockMode == LockMode::EXCLUSIVE);
                    second->state = LockState::ACQUIRED;
                    ++lockList.acquire_count;
                    --lockList.wait_count;
                    second->signal();
                    // std::cout << "sxlock -> ";
                }
            }
            else
            {
                if (lockList.acquire_count == 1)
                {
                    // SLock인가 XLock인가?
                    /*
                        만약 front가 SLock일 경우
                            유일한 XLock을 지웠다 -> mode를 SLock으로
                            XLock이 남아있다 -> 변경 X
                        XLock일 경우
                            변경 X

                    */
                    if (lockList.locks.front()->lockMode == LockMode::SHARED)
                    {
                        bool flag = true;
                        auto it = std::next(lockList.locks.begin(), 1);
                        for (int i = 0; i < lockList.wait_count; ++i)
                        {
                            if (it->get()->lockMode == LockMode::EXCLUSIVE)
                            {
                                flag = false;
                                break;
                            }
                        }
                        if (flag)
                        {
                            lockList.mode = LockMode::SHARED;
                        }
                    }
                }
                else
                {
                    // 무조건 SLock이다
                    lockList.mode = LockMode::SHARED;
                }
            }

            // std::cout << "acquire > 0. end.\n";
            return true;
        }

        if (lockList.wait_count == 0)
        {
            lock_table.erase(hash);
            // std::cout << "wait == 0. end.\n";
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
            // std::cout << "front xlock. end.\n";
            return true;
        }

        lockList.mode = LockMode::SHARED;

        for (auto& it : lockList.locks)
        {
            if (it->lockMode == LockMode::EXCLUSIVE)
            {
                lockList.mode = LockMode::EXCLUSIVE;
                // std::cout << "xlock acquired -> ";
                break;
            }
            it->state = LockState::ACQUIRED;
            ++lockList.acquire_count;
            --lockList.wait_count;
            it->signal();
            // std::cout << "slock acquired -> ";
        }
        // std::cout << "return true. mode: " << lockList.mode << '\n';
        ;
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

lock_t::lock_t(LockHash hash, LockMode lockMode, int ownerTransactionID)
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
    : mode(rhs.mode),
      wait_count(rhs.wait_count),
      acquire_count(rhs.acquire_count)
{
    // Do nothing
}