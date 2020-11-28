#include "transaction_manager.hpp"

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

bool TransactionManager::abort(int transaction_id)
{
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

Transaction::Transaction() : transactionID(-1)
{
    // Do nothing
}

Transaction::Transaction(int id) : transactionID(id)
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
    // TODO:
    return true;
}