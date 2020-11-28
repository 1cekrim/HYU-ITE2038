#include "log_manager.hpp"

int LogManager::log(int transaction_id, LogType type, LockHash hash,
                    const Record& before, const Record& after)
{
    std::unique_lock<std::mutex> crit{ mtx };
    int prev_number = invalid_log_number;
    int number = ++counter;
    if (auto it = last_log.find(transaction_id); it == last_log.end())
    {
        prev_number = it->second;
    }

    
}

std::vector<LogStruct> LogManager::trace_log(int transaction_id)
{
}