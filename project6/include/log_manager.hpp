#ifndef __LOG_MANAGER_HPP__
#define __LOG_MANAGER_HPP__

#include <fcntl.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>
#include <variant>

#include "buffer_manager.hpp"
#include "page.hpp"

constexpr auto LOG_BUFFER_SIZE = 1000000;

enum class LogType : int32_t
{
    BEGIN = 0,
    UPDATE = 1,
    COMMIT = 2,
    ROLLBACK = 3,
    COMPENSATE = 4
};

enum class RecoveryMode
{
    NORMAL = 0,
    REDO_CRASH = 1,
    UNDO_CRASH = 2
};

struct CommonLogRecord
{
    int64_t lsn;
    int64_t prev_lsn;
    int32_t transaction_id;
    LogType type;
} __attribute__((packed));

struct UpdateLogRecord : CommonLogRecord
{
    int32_t table_id;
    int64_t page_number;
    int32_t offset;
    int32_t data_length;
    valType old_image;
    valType new_image;
} __attribute__((packed));

struct CompensateLogRecord : UpdateLogRecord
{
    int64_t next_undo_lsn;
} __attribute__((packed));

using LogRecord =
    std::variant<CommonLogRecord, UpdateLogRecord, CompensateLogRecord>;

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
}

class LogBuffer
{
 public:
    LogBuffer();
    void open(const std::string& log_path);
    // buffer에 LogRecord를 추가한 뒤, 추가된 rec의 lsn을 반환
    int64_t append(const LogRecord& rec);
    void flush();
    void reset();

 private:
    std::atomic<int> last_lsn;
    std::atomic<int> buffer_tail;
    std::mutex lsn_tail_latch;
    std::array<LogRecord, LOG_BUFFER_SIZE> buffer;
    std::array<std::mutex, LOG_BUFFER_SIZE> buffer_latch;
    std::mutex flush_latch;
    int fd;
};

class LogReader
{
 public:
    LogReader(std::string& log_path);

 private:
};

class LogManager
{
 public:
    static LogManager& instance()
    {
        static LogManager logManager;
        return logManager;
    }

    int begin_log(int transaction_id);
    int commit_log(int transaction_id);
    int update_log(int transaction_id, int table_id, pagenum_t page_number,
                   int offset, int data_length, const valType& old_image,
                   const valType& new_image);

    void open(const std::string& log_path, const std::string& logmsg_path);

    bool recovery(RecoveryMode mode, int log_num);

    int rollback(int transaction_id);

    void make_dirty(pagenum_t pagenum);

    void reset();

    static constexpr auto invalid_log_number = -1;

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
        inline void begin(int64_t lsn, int trx_id);
        inline void update(int64_t lsn, int trx_id);
        inline void commit(int64_t lsn, int trx_id);
        inline void rollback(int64_t lsn, int trx_id);
        inline void compensate(int64_t lsn, int64_t next_undo_lsn);
        inline void consider_redo(int64_t lsn, int trx_id);
        inline void redo_pass_end();
        inline void flush_with_sync();

     private:
        int fd;
        std::FILE* fp;
    };
};

#endif /* __LOG_MANAGER_HPP__*/