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

std::ostream& operator<<(std::ostream& os, const LockState& dt)
{
    switch (dt)
    {
        case LockState::INVALID:
            os << "INVALID";
            break;
        case LockState::ACQUIRED:
            os << "ACQUIRED";
            break;
        case LockState::WAITING:
            os << "WAITING";
            break;
        case LockState::ABORTED:
            os << "ABORTED";
            break;
    }
    return os;
}

lock_t* LockManager::lock_acquire(int table_id, int64_t key, int trx_id,
                                  LockMode mode)
{
    std::unique_ptr<lock_t> lock =
        std::make_unique<lock_t>(LockHash(table_id, key), mode, trx_id);
    auto lock_ptr = lock.get();
    lock->state = LockState::WAITING;

    LockHash hash { table_id, key };
    if (!lock)
    {
        return nullptr;
    }

    {
        std::unique_lock<std::mutex> crit { mtx };
        if (auto it = lock_table.find(hash);
            it == lock_table.end() || (it->second.mode == LockMode::SHARED &&
                                       lock->lockMode == LockMode::SHARED))
        {
            auto& list = lock_table[hash];

            lock->state = LockState::ACQUIRED;
            // lock->signal();
            auto& transaction = TransactionManager::instance().get(trx_id);
            transaction.state = TransactionState::RUNNING;
            lock->locked = false;
            list.mode = mode;
            list.locks.emplace_front(std::move(lock));
            ++list.acquire_count;

            return lock_ptr;
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
        table.locks.emplace_back(std::move(lock));
        ++table.wait_count;
    }

    if (deadlock_detection(trx_id))
    {
        // TransactionManager::instance().abort(trx_id);
        lock_release(lock_ptr);
        return nullptr;
    }

    return lock_ptr;
}

lock_t* LockManager::lock_upgrade(int table_id, int64_t key, int trx_id,
                                  LockMode mode)
{
    CHECK_RET(mode == LockMode::EXCLUSIVE, nullptr);
    auto lock = std::make_unique<lock_t>(LockHash(table_id, key), mode, trx_id);
    lock->state = LockState::WAITING;
    auto lock_ptr = lock.get();
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
            // auto& transaction = TransactionManager::instance().get(trx_id);
            // transaction.state = TransactionState::RUNNING;
            lock->locked = false;
            lock_list.mode = mode;
            lock_list.locks.emplace_back(std::move(lock));
            ++lock_list.acquire_count;
            return lock_ptr;
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
        lock_list.locks.emplace_back(std::move(lock));
        ++lock_list.wait_count;
    }

    if (deadlock_detection(trx_id))
    {
        // TransactionManager::instance().abort(trx_id);
        lock_release(lock_ptr);
        return nullptr;
    }

    return lock_ptr;
}

void LockManager::lock_wait(lock_t* lock_obj)
{
    while (lock_obj->wait())
    {
        std::this_thread::yield();
    }

    std::unique_lock<std::mutex> trx_latch {
        TransactionManager::instance().mtx
    };
    std::unique_lock<std::mutex> crit {
        TransactionManager::instance().get(lock_obj->ownerTransactionID).mtx
    };
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
                graph[wait_id].next.insert(acquire_id);
                graph[wait_id].visited = false;
            }
        }
    }

    bool deadlock = false;
    dfs(now_transaction_id, graph, deadlock);

    return deadlock;
}

bool LockManager::lock_release(lock_t* lock_obj, bool abort)
{
    {
        std::unique_lock<std::mutex> crit { mtx, std::defer_lock };
        if (!abort)
        {
            crit.lock();
        }
        LockHash hash = lock_obj->hash;
        auto& lockList = lock_table[hash];
        auto& table = lockList.locks;

        auto iter =
            std::find_if(table.begin(), table.end(), [lock_obj](auto& p) {
                return p.get() == lock_obj;
            });

        // CHECK(iter != table.end());
        // 없는거 release 하려하면 그냥 통과
        /*
         왜 lockList에 없는 lock을 지우려는 상황이 발생하는가?
         이는 lock_upgrade의 구현 때문이다.
         lock_upgrade의 return value가 transaction의 lock list에 추가된다.
         그런데 만약, lock list에 해당 트랜잭션의 slock만 존재할 경우,
         lock_upgrade는 최적화를 위해 해당 slock을 그냥 xlock으로 변환한다.
         변환한 이후, 변환된 xlock(slock이었던 것)의 shared_ptr을 반환한다.
         그러면 이게 트랜잭션의 lock list에 emplace 되는데, 이건 이미 트랜잭션의
         lock list에 추가되어 있는 상태다! 이렇게 lock list에 중복된
         shared_ptr이 들어가고, 해당 트랜잭션이 abort 된다면
         transaction::abort에서 이미 lock_release된 lock을 lock_release 하게
         된다. 기존 구현에서는, lock list에 존재하지 않는 lock을 release 하려는
         상황이 발생했을 때, 논리적 오류로 취급했다. 하지만 위와 같은 상황을
         처리하기 위해 논리적 오류가 아니라고 결론지었다. 트랜잭션의 lock list에
         emplace 할 때마다 중복 검사하는 것은 비효율적이다. 다른 부분의 로직에
         문제가 없는 걸 확인했으니 release에서 처리해도 될 것 같다.
        */
        if (iter == table.end())
        {
            return true;
        }

        if (iter->get()->state == LockState::ACQUIRED)
        {
            --lockList.acquire_count;
        }
        else
        {
            // wait 중인걸 그냥 lock_release 해도 되나?
            // TODO: abort 될때 처리...
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
                }
            }

            bool flag = true;
            for (const auto& it : lockList.locks)
            {
                if (it->lockMode == LockMode::EXCLUSIVE)
                {
                    flag = false;
                    break;
                }
            }

            if (flag)
            {
                lockList.mode = LockMode::SHARED;
            }
            else
            {
                lockList.mode = LockMode::EXCLUSIVE;
            }

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
    auto& transaction = TransactionManager::instance().get(ownerTransactionID);
    std::unique_lock<std::mutex> crit { transaction.mtx };

    locked = false;
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