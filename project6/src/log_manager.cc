#include "log_manager.hpp"

LogBuffer::LogBuffer()
{
    reset();
}

void LogBuffer::reset()
{
    {
        std::unique_lock<std::mutex> value_latch_lock { value_latch };
        last_lsn = 0;
        flushed_lsn = INVALID_LSN;
        buffer_tail = 0;
    }
}

void LogBuffer::open(const std::string& log_path)
{
    fd = ::open(log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
}

int64_t LogBuffer::append(const LogRecord& record)
{
    std::unique_lock<std::mutex> value_latch_lock { value_latch };
    int my_lsn = last_lsn;
    last_lsn += get_log_record_size(record);
    int buffer_index = buffer_tail++;
    // TODO: buffer_index 넘치면 flush
    value_latch_lock.unlock();

    std::unique_lock<std::mutex> buffer_latch_lock {
        buffer_latch[buffer_index]
    };
    buffer[buffer_index] = record;
    std::visit(
        [&](auto&& rec) {
            rec.lsn = my_lsn;
        },
        buffer[buffer_index]);

    return my_lsn;
}

void LogBuffer::flush()
{
    // flush는 한번에 한 스레드에서만 호출 가능하다.
    std::unique_lock<std::mutex> crit { flush_latch };

    // 버퍼에 존재하는 모든 로그의 latch를 잠근다.
    // 로그를 작성하는 스레드늨 buffer_tail을 증가시킬 때 buffer_latch를 걸고,
    // 로그 작성이 완료된 후에 buffer_latch를 푼다. 즉, flush에서 모든
    // buffer_latch의 lock을 얻을 때까지 대기한다는 건, 현재 작성중인 모든
    // 로그의 작성이 완료될 때까지 대기한다는 의미이다.
    for (int i = 0; i < buffer_tail; ++i)
    {
        buffer_latch[i].lock();
    }

    // O_APPEND 옵션이 있어 로그가 파일 맨 뒤에 작성되므로 flushed_lsn을
    // 신경쓰지 않아도 된다.
    for (int i = 0; i < buffer_tail; ++i)
    {
        std::visit(
            [&](auto&& rec) {
                write(fd, &rec, sizeof(rec));
            },
            buffer[i]);
    }

    // log buffer를 초기화한다.
    buffer_tail = 0;

    // latch를 푼다.
    for (int i = 0; i < buffer_tail; ++i)
    {
        buffer_latch[i].unlock();
    }
}

void LogManager::open(const std::string& log_path,
                      const std::string& logmsg_path)
{
    buffer.open(log_path);
    this->log_path = log_path;
    this->logmsg_path = logmsg_path;
}

int64_t LogManager::log_wrapper(int transaction_id, const LogRecord& rec)
{
    std::unique_lock<std::mutex> trx_table_latch_lock { trx_table_latch };
    trx_table_latch_lock.unlock();
    auto lsn = buffer.append(rec);
    trx_table_latch_lock.lock();
    trx_table[transaction_id] = lsn;
    return lsn;
}

int64_t LogManager::begin_log(int transaction_id)
{
    CommonLogRecord record { INVALID_LSN, INVALID_LSN, transaction_id,
                             LogType::BEGIN };
    return log_wrapper(transaction_id, record);
}

int64_t LogManager::commit_log(int transaction_id)
{
    CommonLogRecord record { INVALID_LSN, trx_table[transaction_id], transaction_id,
                             LogType::COMMIT };
    return log_wrapper(transaction_id, record);
}

int64_t LogManager::update_log(int transaction_id, int table_id,
                               pagenum_t page_number, int offset,
                               int data_length, const valType& old_image,
                               const valType& new_image)
{
    UpdateLogRecord record { INVALID_LSN,
                             trx_table[transaction_id],
                             transaction_id,
                             LogType::UPDATE,
                             table_id,
                             page_number,
                             offset,
                             data_length,
                             old_image,
                             new_image };
    return log_wrapper(transaction_id, record);
}

bool LogManager::rollback(int transaction_id)
{
    auto it = trx_table.find(transaction_id);
    if (it == trx_table.end())
    {
        // 트랜잭션이 begin 할때 무조건 begin 로그가 작성되는데, trx_table에 trx id가 없다는 건 trx id가 invalid 하다는 의미
        return false;
    }

    int now = it->second;

    buffer.flush();
    
    while (now != INVALID_LSN)
    {
        // 만약 now가 buffer에 존재할 경우는 생각할 필요가 없다.
        // 왜냐하면 rollback을 한다는 것은 trx가 abort 됐다는 것이고,
        // 이 때 buffer를 flush 해야하기 때문...
        
    }
}

LogManager::Message::Message(const std::string& logmsg_path)
    : fd(::open(logmsg_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666)),
      fp(fdopen(fd, "w+"))
{
    // Do nothing
}

inline void LogManager::Message::analysis_start()
{
    fprintf(fp, "[ANALYSIS] Analysis pass start\n");
}

inline void LogManager::Message::analysis_end(const std::vector<int>& winners,
                                              const std::vector<int>& losers)
{
    fprintf(fp, "[ANALYSIS] Analysis success. Winner:");
    for (auto& i : winners)
    {
        fprintf(fp, " %d", i);
    }
    fprintf(fp, ", Loser:");
    for (auto& i : losers)
    {
        fprintf(fp, " %d", i);
    }
    fprintf(fp, "\n");
}

inline void LogManager::Message::redo_pass_start()
{
    fprintf(fp, "[REDO(UNDO)] Redo(Undo) pass start\n");
}

inline void LogManager::Message::begin(int64_t lsn, int trx_id)
{
    fprintf(fp, "LSN %lu[BEGIN] Transaction id %d\n", lsn, trx_id);
}

inline void LogManager::Message::update(int64_t lsn, int trx_id)
{
    fprintf(fp, "LSN %lu[UPDATE] Transaction id %d redo(undo) apply\n", lsn,
            trx_id);
}

inline void LogManager::Message::commit(int64_t lsn, int trx_id)
{
    fprintf(fp, "LSN %lu[COMMIT] Transaction id %d\n", lsn, trx_id);
}

inline void LogManager::Message::rollback(int64_t lsn, int trx_id)
{
    fprintf(fp, "LSN %lu[ROLLBACK] Transaction id %d\n", lsn, trx_id);
}

inline void LogManager::Message::compensate(int64_t lsn, int64_t next_undo_lsn)
{
    fprintf(fp, "LSN %lu[CLR] next undo lsn %lu\n", lsn, next_undo_lsn);
}

inline void LogManager::Message::consider_redo(int64_t lsn, int trx_id)
{
    fprintf(fp, "LSN %lu[CONSIDER - REDO] Transaction id %d\n", lsn, trx_id);
}

inline void LogManager::Message::redo_pass_end()
{
    fprintf(fp, "[REDO(UNDO)] Redo(Undo) pass end\n");
}

inline void LogManager::Message::flush_with_sync()
{
    std::fflush(fp);
    fsync(fd);
}