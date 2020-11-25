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

bool TransactionManager::commit(int id)
{
    std::unique_lock<std::mutex> lock(mtx);
    auto it = transactions.find(id);
    CHECK(it != transactions.end());

    CHECK(it->second.commit());
    transactions.erase(it);

    return true;
}