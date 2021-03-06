#include "transaction_manager.hpp"

#include <algorithm>
#include <iostream>

#include "log_manager.hpp"
#include "logger.hpp"
#include "table_manager.hpp"

int TransactionManager::begin()
{
    std::unique_lock<std::mutex> trx_latch { mtx };
    int id = ++counter;

    if (transactions.find(id) != transactions.end())
    {
        return invliad_transaction_id;
    }

    transactions.emplace(id, Transaction(id));
    LogManager::instance().begin_log(id);
    return id;
}

Transaction& TransactionManager::get(int transaction_id)
{
    return transactions[transaction_id];
}

void TransactionManager::lock_wait(lock_t* lock)
{
    LockManager::instance().lock_wait(lock);
}

LockAcquireResult TransactionManager::lock_acquire(int table_id, int64_t key,
                                                   int trx_id, LockMode mode)
{
    LockHash hash { table_id, key };

    auto transaction = transactions.find(trx_id);
    if (transaction == transactions.end())
    {
        return { nullptr, LockState::INVALID };
    }

    auto& locks = transaction->second.locks;
    if (auto count = std::count_if(locks.begin(), locks.end(),
                                   [&hash](auto& t) {
                                       return std::get<0>(t) == hash;
                                   });
        count > 0)
    {
        auto aleady_lock_mode =
            count == 1 ? LockMode::SHARED : LockMode::EXCLUSIVE;
        // 왜 아래의 if문과 같은 로직이 가능한가?
        /*
        지금 이 영역은 현재 transaction이 lock을 획득하고 있을 때 실행된다
        기존에 획득한 lock이 새로 획득할 lock보다 더 강한 mode라면 추가로
        획득하지 않아도 된다 만약 새로 추가할 lock의 mode가 SHARED 라면 lock을
        획득할 필요가 없다 만약 기본의 mode가 EXCLUSIVE라면 새로 추가할 mode와
        상관없이 lock을 획득할 필요가 없다
        */
        if (mode == LockMode::SHARED || aleady_lock_mode == LockMode::EXCLUSIVE)
        {
            // TODO: 이거 nullptr 리턴해도 되나?
            return { nullptr, LockState::ACQUIRED };
        }

        /*
        이 영역에 도달했다는 것은, 현재 트랜잭션이 SLock만을 획득했고, XLock을
        획득하려 시도한다는 의미다. 이 경우 LockManager의 lock_upgrade 메소드를
        통해 XLock을 추가해 준다.
        */
        auto xlock =
            LockManager::instance().lock_upgrade(table_id, key, trx_id, mode);

        if (!xlock)
        {
            return { nullptr, LockState::ABORTED };
        }

        locks.emplace_back(hash, xlock);

        if (xlock->state == LockState::WAITING)
        {
            // Acquire the transaction latch
            return { xlock, LockState::WAITING };
        }

        return { xlock, LockState::ACQUIRED };
    }

    auto new_lock =
        LockManager::instance().lock_acquire(table_id, key, trx_id, mode);

    if (!new_lock)
    {
        return { nullptr, LockState::ABORTED };
    }

    locks.emplace_back(hash, new_lock);

    if (new_lock->state == LockState::WAITING)
    {
        // Acquire the transaction latch
        return { new_lock, LockState::WAITING };
    }

    return { new_lock, LockState::ACQUIRED };
}

int TransactionManager::abort(int transaction_id)
{
    auto it = transactions.find(transaction_id);
    if (it == transactions.end())
    {
        return 0;
    }

    CHECK_RET(it->second.abort(), 0);
    transactions.erase(it);

    return transaction_id;
}

int TransactionManager::commit(int id)
{
    std::unique_lock<std::mutex> trx_latch { mtx };

    auto it = transactions.find(id);
    if (it == transactions.end())
    {
        return id;
    }

    LogManager::instance().commit_log(id);

    CHECK_RET(it->second.commit(), 0);
    transactions.erase(it);

    return id;
}

void TransactionManager::reset()
{
    std::unique_lock<std::mutex> trx_latch { mtx };
    counter = 0;
    transactions.clear();
}

const std::unordered_map<int, Transaction>&
TransactionManager::get_transactions() const
{
    return transactions;
}

Transaction::Transaction() : transactionID(-1)
{
    // Do nothing
}

Transaction::Transaction(int id)
    : transactionID(id),
      state(TransactionState::RUNNING)
{
    // Do nothing
}

Transaction::Transaction(const Transaction& rhs)
    : transactionID(rhs.transactionID),
      state(rhs.state.load())
{
    // Do nothing
}

bool Transaction::commit()
{
    // TODO: log commit
    CHECK(LogManager::instance().flush());
    return lock_release();
}

bool Transaction::lock_release(bool abort)
{
    for (auto& it : locks)
    {
        CHECK(LockManager::instance().lock_release(std::get<1>(it), abort));
    }

    locks.clear();
    return true;
}

bool Transaction::abort()
{
    state = TransactionState::ABORTED;
    CHECK(LogManager::instance().rollback(transactionID));

    return lock_release(true);
}