#ifndef __LOCK_MANAGER_HPP__
#define __LOCK_MANAGER_HPP__

#include <atomic>
#include <list>
#include <map>
#include <memory>
#include <mutex>

enum class LockMode
{
    SHARED = 0,
    EXCLUSIVE = 1,
    EMPTY = 2
};

enum class LockState
{
    INVALID,
    WAITING,
    ACQUIRED
};

struct lock_t
{
    int table_id;
    int64_t key;
    std::atomic<bool> locked;
    std::atomic<LockMode> lockMode;
    std::atomic<LockState> state;
    int ownerTransactionID;

    lock_t(int table_id, int64_t key, LockMode lockMode, int ownerTransactionID);

    bool wait() const;
    void signal();
};

struct LockList
{
    std::list<std::shared_ptr<lock_t>> locks;
    std::atomic<LockMode> mode = LockMode::EMPTY;
    std::atomic<int> wait_count;
    std::atomic<int> acquire_count;
    LockList();
    LockList(const LockList& rhs);
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
    bool lock_release(std::shared_ptr<lock_t> lock_obj);
    const std::map<LockHash, LockList>& get_table() const;
    bool deadlock_detection();

    void reset();

 private:
    std::mutex mtx;
    std::map<LockHash, LockList> lock_table;

    LockManager() = default;
};

#endif /* __LOCK_MANAGER_HPP__*/