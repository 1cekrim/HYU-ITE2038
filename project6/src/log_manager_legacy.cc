#include "log_manager_legacy.hpp"

int LogManagerLegacy::log(int transaction_id, LogTypeLegacy type, LockHash hash,
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

int LogManagerLegacy::log(int transaction_id, LogTypeLegacy type)
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

void LogManagerLegacy::reset()
{
    std::unique_lock<std::mutex> crit { mtx };
    logs.clear();
    last_log.clear();
    counter = 0;
}

std::vector<LogStructLegacy> LogManagerLegacy::trace_log(int transaction_id)
{
    std::vector<LogStructLegacy> result;
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

LogStructLegacy::LogStructLegacy(int prev_number, int number, int transaction, LogTypeLegacy type,
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

LogStructLegacy::LogStructLegacy(int prev_number, int number, int transaction, LogTypeLegacy type)
    : prev_number(prev_number),
      number(number),
      transaction(transaction),
      type(type)
{
    // Do nothing
}