#ifndef __LOG_MANAGER_LEGACY_HPP__
#define __LOG_MANAGER_LEGACY_HPP__

#include <mutex>
#include <unordered_map>
#include <vector>

#include "bptree.hpp"
#include "lock_manager.hpp"

enum class LogTypeLegacy
{
    INVALID,
    BEGIN,
    SELECT,
    UPDATE,
    ABORT,
    COMMIT,
};

struct LogStructLegacy
{
    int prev_number;
    int number;
    int transaction;
    LogTypeLegacy type;
    LockHash hash;
    Record before;
    Record after;
    LogStructLegacy(int prev_number, int number, int transaction, LogTypeLegacy type,
              const LockHash& hash, const Record& before, const Record& after);
    LogStructLegacy(int prev_number, int number, int transaction, LogTypeLegacy type);
};

class LogManagerLegacy
{
 public:
    static LogManagerLegacy& instance()
    {
        static LogManagerLegacy logManager;
        return logManager;
    }
    int log(int transaction_id, LogTypeLegacy type, LockHash hash,
            const Record& before, const Record& after);

    int log(int transaction_id, LogTypeLegacy type);

    std::vector<LogStructLegacy> trace_log(int transaction_id);

    void reset();

    static constexpr auto invalid_log_number = -1;

 private:
    std::mutex mtx;
    std::vector<LogStructLegacy> logs;
    std::unordered_map<int, int> last_log;
    int counter;
    LogManagerLegacy() = default;
};

#endif /* __LOG_MANAGER_LEGACY_HPP__*/