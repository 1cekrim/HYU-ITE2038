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

    logs.emplace_back(prev_number, number, transaction_id, type, hash, before,
                      after);

    return number;
}

int LogManager::log(int transaction_id, LogType type)
{
    std::unique_lock<std::mutex> crit{ mtx };
    int prev_number = invalid_log_number;
    int number = ++counter;
    if (auto it = last_log.find(transaction_id); it == last_log.end())
    {
        prev_number = it->second;
    }

    logs.emplace_back(prev_number, number, transaction_id, type);

    return number;
}

std::vector<LogStruct> LogManager::trace_log(int transaction_id)
{
    std::vector<LogStruct> result;
    auto it = last_log.find(transaction_id);
    if (it == last_log.end())
    {
        return {};
    }

    int now = it->second;

    while (now != invalid_log_number)
    {
        result.emplace_back(logs[now]);
        now = logs[now].prev_number;
    }

    return {};
}