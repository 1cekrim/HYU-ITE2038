#include "log_manager.hpp"

int LogManager::log(int transaction_id, LogType type, LockHash hash,
                    const Record& before, const Record& after)
{
    std::unique_lock<std::mutex> crit { mtx };
    int prev_number = invalid_log_number;
    int number = counter++;
    if (auto it = last_log.find(transaction_id); it != last_log.end())
    {
        prev_number = it->second;
    }

    logs.emplace_back(prev_number, number, transaction_id, type, hash, before,
                      after);
    last_log.insert_or_assign(transaction_id, number);

    return number;
}

int LogManager::log(int transaction_id, LogType type)
{
    std::unique_lock<std::mutex> crit { mtx };
    int prev_number = invalid_log_number;
    int number = counter++;
    if (auto it = last_log.find(transaction_id); it != last_log.end())
    {
        prev_number = it->second;
    }

    logs.emplace_back(prev_number, number, transaction_id, type);
    last_log.insert_or_assign(transaction_id, number);

    return number;
}

void LogManager::reset()
{
    std::unique_lock<std::mutex> crit { mtx };
    logs.clear();
    last_log.clear();
    counter = 0;
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

    return result;
}

LogStruct::LogStruct(int prev_number, int number, int transaction, LogType type,
                     const LockHash& hash, const Record& before,
                     const Record& after)
    : prev_number(prev_number),
      number(number),
      transaction(transaction),
      type(type),
      hash(hash),
      before(before),
      after(after)
{
    // Do nothing
}

LogStruct::LogStruct(int prev_number, int number, int transaction, LogType type)
    : prev_number(prev_number),
      number(number),
      transaction(transaction),
      type(type)
{
    // Do nothing
}