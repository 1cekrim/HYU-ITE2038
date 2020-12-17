#ifndef __LOG_MANAGER_HPP__
#define __LOG_MANAGER_HPP__

#include <fcntl.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>
#include <tuple>
#include <variant>

#include "buffer_manager.hpp"
#include "page.hpp"

constexpr auto LOG_BUFFER_SIZE = 10000;

enum class LogType : int32_t
{
    BEGIN = 0,
    UPDATE = 1,
    COMMIT = 2,
    ROLLBACK = 3,
    COMPENSATE = 4,
    INVALID
};

std::ostream& operator<<(std::ostream& os, const LogType& dt);

enum class RecoveryMode
{
    NORMAL = 0,
    REDO_CRASH = 1,
    UNDO_CRASH = 2
};

struct LogHeader
{
    uint32_t header = 0xffeeaaff;
    int64_t start_lsn;
};

struct CommonLogRecord
{
    int64_t lsn;
    int64_t prev_lsn;
    int32_t transaction_id;
    LogType type = LogType::INVALID;
    int32_t log_size;
} __attribute__((packed));

struct UpdateLogRecord
{
    int64_t lsn;
    int64_t prev_lsn;
    int32_t transaction_id;
    LogType type = LogType::INVALID;
    int32_t table_id;
    int64_t page_number;
    int32_t offset;
    int32_t data_length;
    valType old_image;
    valType new_image;
    int32_t log_size;
} __attribute__((packed));

struct CompensateLogRecord
{
    int64_t lsn;
    int64_t prev_lsn;
    int32_t transaction_id;
    LogType type = LogType::INVALID;
    int32_t table_id;
    int64_t page_number;
    int32_t offset;
    int32_t data_length;
    valType old_image;
    valType new_image;
    int64_t next_undo_lsn;
    int32_t log_size;
} __attribute__((packed));

std::ostream& operator<<(std::ostream& os, const CommonLogRecord& dt);
std::ostream& operator<<(std::ostream& os, const UpdateLogRecord& dt);
std::ostream& operator<<(std::ostream& os, const CompensateLogRecord& dt);

using LogRecord =
    std::variant<CommonLogRecord, UpdateLogRecord, CompensateLogRecord>;

constexpr auto INVALID_LSN = -1;

constexpr std::size_t get_log_record_size(const LogRecord& rec)
{
    switch (rec.index())
    {
        case 0:
            return sizeof(CommonLogRecord);
        case 1:
            return sizeof(UpdateLogRecord);
        case 2:
            return sizeof(CompensateLogRecord);
    }
    exit(-1);
    return 0;
}

constexpr int64_t get_log_record_lsn(const LogRecord& rec)
{
    switch (rec.index())
    {
        case 0:
            return std::get<CommonLogRecord>(rec).lsn;
        case 1:
            return std::get<UpdateLogRecord>(rec).lsn;
        case 2:
            return std::get<CompensateLogRecord>(rec).lsn;
    }
    exit(-1);
    return 0;
}

constexpr int32_t get_log_record_trx(const LogRecord& rec)
{
    switch (rec.index())
    {
        case 0:
            return std::get<CommonLogRecord>(rec).transaction_id;
        case 1:
            return std::get<UpdateLogRecord>(rec).transaction_id;
        case 2:
            return std::get<CompensateLogRecord>(rec).transaction_id;
    }
    exit(-1);
    return 0;
}

class LogBuffer
{
 public:
    LogBuffer();
    void open(const std::string& log_path);
    // buffer에 LogRecord를 추가한 뒤, 추가된 rec의 lsn을 반환
    int64_t append(const LogRecord& rec);
    void flush();
    bool flush_prev_lsn(int64_t page_lsn);
    void reset();
    void truncate();
    std::atomic<int> last_lsn;

 private:
    std::atomic<int> flushed_lsn;
    std::mutex flush_latch;
    std::array<std::mutex, LOG_BUFFER_SIZE> buffer_latch;
    std::atomic<int> buffer_head;
    std::atomic<int> buffer_tail;
    std::array<LogRecord, LOG_BUFFER_SIZE> buffer;
    std::mutex value_latch;
    int fd;

    void flush_without_value_latch_lock();
};

class LogReader
{
 public:
    LogReader(const std::string& log_path, int64_t start_lsn);
    LogReader(const std::string& log_path);
    std::tuple<LogType, LogRecord> get(int64_t lsn) const;
    void print() const;
    std::tuple<LogType, LogRecord> next() const;
    std::tuple<LogType, LogRecord> prev() const;
    void set_lsn(int64_t lsn);

 private:
    int fd;
    mutable int64_t now_lsn;
};

class LogManager
{
 public:
    static LogManager& instance()
    {
        static LogManager logManager;
        return logManager;
    }

    // _log 함수들은 추가된 log의 lsn을 반환한다.
    int64_t begin_log(int transaction_id);
    int64_t commit_log(int transaction_id);
    int64_t update_log(int transaction_id, int table_id, pagenum_t page_number,
                       int offset, int data_length, const valType& old_image,
                       const valType& new_image);

    int64_t log_wrapper(int transaction_id, const LogRecord& rec);

    void open(const std::string& log_path, const std::string& logmsg_path);

    bool recovery(RecoveryMode mode, int log_num);

    bool flush_prev_lsn(int64_t page_lsn);

    bool flush();

    bool rollback(int transaction_id);

    void make_dirty(pagenum_t pagenum);

    void reset();

 private:
    std::string log_path;
    std::string logmsg_path;

    LogBuffer buffer;

    std::mutex trx_table_latch;
    std::unordered_map<int, int> trx_table;

    std::mutex dirty_table_latch;
    std::unordered_map<int, int> dirty_table;

    LogManager() = default;

    class Message
    {
     public:
        Message(const std::string& logmsg_path);
        inline void analysis_start();
        inline void analysis_end(const std::vector<int>& winners,
                                 const std::vector<int>& losers);

        inline void redo_pass_start();
        inline void undo_pass_start();
        inline void begin(int64_t lsn, int trx_id);
        inline void update_redo(int64_t lsn, int trx_id);
        inline void update_undo(int64_t lsn, int trx_id);
        inline void commit(int64_t lsn, int trx_id);
        inline void rollback(int64_t lsn, int trx_id);
        inline void compensate(int64_t lsn, int64_t next_undo_lsn);
        inline void consider_redo_update(int64_t lsn, int trx_id);
        inline void consider_redo_compensate(int64_t lsn, int trx_id);
        inline void redo_pass_end();
        inline void undo_pass_end();
        inline void flush_with_sync();

     private:
        int fd;
        std::FILE* fp;
    };
};

#endif /* __LOG_MANAGER_HPP__*/