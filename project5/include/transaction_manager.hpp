#ifndef __TRX_MANAGER_HPP__
#define __TRX_MANAGER_HPP__

#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "lock_manager.hpp"

// class lock_t;
// class LockHash;

enum class TransactionState
{
    INVALID,
    WAITING,
    RUNNING,
    ABORTED
};

struct Transaction
{
    Transaction();
    Transaction(int id);
    Transaction(const Transaction& rhs);
    bool commit();
    bool abort();
    bool lock_release();
    int transactionID;
    std::atomic<TransactionState> state;
    std::list<std::tuple<LockHash, std::shared_ptr<lock_t>>> locks;
    std::mutex mtx;
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
    Transaction& get(int transaction_id);
    const std::unordered_map<int, Transaction>& get_transactions() const;
    bool abort(int transaction_id);
    void reset();

    bool lock_acquire(int table_id, int key, int record_index, int trx_id, LockMode mode);

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