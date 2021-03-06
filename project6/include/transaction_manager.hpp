#ifndef __TRX_MANAGER_HPP__
#define __TRX_MANAGER_HPP__

#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "buffer_manager.hpp"
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
    bool lock_release(bool abort = false);
    int transactionID;
    std::atomic<TransactionState> state;
    std::list<std::tuple<LockHash, lock_t*>> locks;
    std::mutex mtx;
};

struct LockAcquireResult
{
    lock_t* lock;
    LockState state;
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
    int commit(int id);
    Transaction& get(int transaction_id);
    const std::unordered_map<int, Transaction>& get_transactions() const;
    int abort(int transaction_id);
    void reset();

    LockAcquireResult lock_acquire(int table_id, int64_t key, int trx_id,
                                   LockMode mode);
    void lock_wait(lock_t* lock);

    static constexpr int invliad_transaction_id = 0;

    std::mutex mtx;

 private:
    std::atomic<int> counter;
    std::unordered_map<int, Transaction> transactions;

    TransactionManager() : counter(0)
    {
        // Do nothing
    }
};

#endif /* __TRX_MANAGER_HPP__*/