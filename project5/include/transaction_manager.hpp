#ifndef __TRX_MANAGER_HPP__
#define __TRX_MANAGER_HPP__

#include <atomic>
#include <mutex>
#include <unordered_map>

class Transaction
{
 public:
    Transaction(int id);
    bool commit();
 private:
    int transactionID;
};

class TransactionManager
{
 public:
    static TransactionManager& instance()
    {
        static TransactionManager transactionManager;
        return transactionManager;
    }
    int begin();
    bool commit(int id);

    static constexpr int invliad_transaction_id = 0;

 private:
    std::atomic<int> counter;
    std::mutex mtx;
    std::unordered_map<int, Transaction> transactions;

    TransactionManager() : counter(0)
    {
        // Do nothing
    }
};

#endif /* __TRX_MANAGER_HPP__*/