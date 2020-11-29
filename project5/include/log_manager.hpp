#ifndef __LOG_MANAGER_HPP__
#define __LOG_MANAGER_HPP__

#include <mutex>
#include <unordered_map>
#include <vector>

#include "bptree.hpp"
#include "lock_manager.hpp"

enum class LogType
{
    INVALID,
    BEGIN,
    SELECT,
    UPDATE,
    ABORT,
    COMMIT,
};

struct LogStruct
{
    int prev_number;
    int number;
    int transaction;
    LogType type;
    LockHash hash;
    Record before;
    Record after;
    LogStruct(int prev_number, int number, int transaction, LogType type,
              const LockHash& hash, const Record& before, const Record& after);
    LogStruct(int prev_number, int number, int transaction, LogType type);
};

class LogManager
{
 public:
    static LogManager& instance()
    {
        static LogManager logManager;
        return logManager;
    }
    int log(int transaction_id, LogType type, LockHash hash,
            const Record& before, const Record& after);

    int log(int transaction_id, LogType type);

    std::vector<LogStruct> trace_log(int transaction_id);

    void reset();

    static constexpr auto invalid_log_number = -1;

 private:
    std::mutex mtx;
    std::vector<LogStruct> logs;
    std::unordered_map<int, int> last_log;
    int counter;
    LogManager() = default;
};

#endif /* __LOG_MANAGER_HPP__*/