#ifndef __LOCK_MANAGER_HPP__
#define __LOCK_MANAGER_HPP__

#include <atomic>
#include <list>
#include <map>
#include <memory>
#include <mutex>

enum class LockMode
{
    shared,
    exclusive
};

struct lock_t
{
    int table_id;
    int64_t key;
    std::atomic<bool> locked;
    LockMode lockMode;
    int ownerTransactionID;

    lock_t(int table_id, int64_t key);

    bool wait() const;
    void signal();
};

struct LockList
{
    std::list<std::shared_ptr<lock_t>> locks;
    LockMode mode;
};

struct LockHash
{
    static constexpr auto invalid_table_id = -1;
    static constexpr auto invalid_key = -1;
    int table_id;
    int64_t key;

    LockHash();
    LockHash(int table_id, int64_t key);

    bool operator<(const LockHash& rhs) const;
    bool operator==(const LockHash& rhs) const;
};

class LockManager
{
 public:
    static LockManager& instance()
    {
        static LockManager lockManager;
        return lockManager;
    }
    std::shared_ptr<lock_t> lock_acquire(int table_id, int64_t key, int trx_id,
                                         LockMode mode);
    int lock_release(std::shared_ptr<lock_t> lock_obj);

 private:
    std::mutex mtx;
    std::map<LockHash, LockList> lock_table;

    LockManager() = default;
};

#endif /* __LOCK_MANAGER_HPP__*/