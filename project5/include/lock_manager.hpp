#ifndef __LOCK_MANAGER_HPP__
#define __LOCK_MANAGER_HPP__

#include <atomic>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>

enum class LockMode
{
    SHARED = 0,
    EXCLUSIVE = 1,
    EMPTY = 2
};
std::ostream& operator<<(std::ostream& os, const LockMode& dt);

enum class LockState
{
    INVALID,
    WAITING,
    ACQUIRED
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

struct lock_t
{
    LockHash hash;
    std::atomic<bool> locked;
    std::atomic<LockMode> lockMode;
    std::atomic<LockState> state;
    int ownerTransactionID;

    lock_t(LockHash hash, LockMode lockMode, int ownerTransactionID);

    bool wait() const;
    void signal();
};

struct LockList
{
    std::list<std::shared_ptr<lock_t>> locks;
    LockMode mode = LockMode::EMPTY;
    int wait_count;
    int acquire_count;
    LockList();
    LockList(const LockList& rhs);

    void print() const
    {
        std::cout << "acquire: " << acquire_count << ", wait: " << wait_count
                  << "mode: "
                  << (mode == LockMode::EXCLUSIVE ? "EXCLUSIVE" : "SHARED")
                  << std::endl;
        for (auto& it : locks)
        {
            if (it->lockMode == LockMode::EXCLUSIVE)
            {
                std::cout << "X";
            }
            else
            {
                std::cout << "S";
            }
            if (it->state == LockState::ACQUIRED)
            {
                std::cout << "!";
            }
            std::cout << it->ownerTransactionID << " ";
        }
        std::cout << std::endl;
    }
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
    std::shared_ptr<lock_t> lock_upgrade(int table_id, int64_t key, int trx_id,
                                         LockMode mode);
    const std::map<LockHash, LockList>& get_table() const;
    bool deadlock_detection(int now_transaction_id);
    void reset();

 private:
    std::mutex mtx;
    std::map<LockHash, LockList> lock_table;

    struct graph_node
    {
        std::set<int> next;
        bool visited;
    };

    void dfs(int now, std::unordered_map<int, graph_node>& graph, bool& stop);

    LockManager() = default;
};

#endif /* __LOCK_MANAGER_HPP__*/