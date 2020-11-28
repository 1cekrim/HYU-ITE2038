#include "transaction_manager.hpp"

#include <algorithm>
#include "logger.hpp"

int TransactionManager::begin()
{
    std::unique_lock<std::mutex> lock(mtx);
    int id = ++counter;

    if (transactions.find(id) != transactions.end())
    {
        return invliad_transaction_id;
    }

    transactions.emplace(id, Transaction(id));

    return id;
}

Transaction& TransactionManager::get(int transaction_id)
{
    return transactions[transaction_id];
}

bool TransactionManager::lock_acquire(int table_id, int key, int record_index,
                                      int trx_id, LockMode mode)
{
    std::unique_lock<std::mutex> crit{ mtx };
    LockHash hash{ table_id, key, record_index };

    auto transaction = transactions.find(trx_id);
    if (transaction == transactions.end())
    {
        return false;
    }

    auto& locks = transaction->second.locks;
    auto& state = transaction->second.state;
    if (auto it =
            std::find_if(locks.begin(), locks.end(),
                         [&hash](auto& t) { return std::get<0>(t) == hash; });
        it != locks.end())
    {
        auto& lock = std::get<1>(*it);
        if (mode != LockMode::EXCLUSIVE || lock->lockMode != LockMode::SHARED)
        {
            return true;
        }

        CHECK(LockManager::instance().lock_release(lock));
        auto new_lock = LockManager::instance().lock_acquire(
            table_id, key, record_index, trx_id, mode);

        if (state == TransactionState::RUNNING)
        {
            lock = new_lock;
        }

        CHECK(state == TransactionState::ABORTED);

        return false;
    }

    auto new_lock = LockManager::instance().lock_acquire(
        table_id, key, record_index, trx_id, mode);

    if (state == TransactionState::RUNNING)
    {
        locks.emplace_back(hash, new_lock);
        return true;
    }

    CHECK(state == TransactionState::ABORTED);

    return false;
}

bool TransactionManager::abort(int transaction_id)
{
    std::unique_lock<std::mutex> lock(mtx);
    auto it = transactions.find(transaction_id);
    CHECK(it != transactions.end());

    CHECK(it->second.abort());
    transactions.erase(it);

    return true;
}

bool TransactionManager::commit(int id)
{
    std::unique_lock<std::mutex> lock(mtx);
    auto it = transactions.find(id);
    CHECK(it != transactions.end());

    CHECK(it->second.commit());
    transactions.erase(it);

    return true;
}

void TransactionManager::reset()
{
    std::unique_lock<std::mutex> lock(mtx);
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
    : transactionID(id), state(TransactionState::RUNNING)
{
    // Do nothing
}

Transaction::Transaction(const Transaction& rhs)
    : transactionID(rhs.transactionID), state(rhs.state.load())
{
    // Do nothing
}

bool Transaction::commit()
{
    std::unique_lock<std::mutex> crit(mtx);
    // TODO: log commit
    return lock_release();
}

bool Transaction::lock_release()
{
    for (auto& it : locks)
    {
        CHECK(LockManager::instance().lock_release(std::get<1>(it)));
    }

    locks.clear();
    return true;
}

bool Transaction::abort()
{
    std::unique_lock<std::mutex> crit(mtx);
    state = TransactionState::ABORTED;
    // log undo
    return lock_release();
}