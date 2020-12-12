#include "log_manager.hpp"

LogBuffer::LogBuffer()
{
    // Do nothing
}

void LogBuffer::open(const std::string& log_path)
{
    fd = ::open(log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
}

void LogBuffer::flush()
{
    // flush는 한번에 한 스레드에서만 호출 가능하다.
    std::unique_lock<std::mutex> crit { flush_latch };

    // 버퍼에 존재하는 모든 로그의 latch를 잠근다.
    // 로그를 작성하는 스레드늨 buffer_tail을 증가시킬 때 buffer_latch를 걸고, 로그 작성이 완료된 후에 buffer_latch를 푼다.
    // 즉, flush에서 모든 buffer_latch의 lock을 얻을 때까지 대기한다는 건, 현재 작성중인 모든 로그의 작성이 완료될 때까지 대기한다는 의미이다.
    for (int i = 0; i < buffer_tail; ++i)
    {
        buffer_latch[i].lock();
    }

    // O_APPEND 옵션이 있어 로그가 파일 맨 뒤에 작성되므로 flushed_lsn을 신경쓰지 않아도 된다.
    for (int i = 0; i < buffer_tail; ++i)
    {
        std::visit([&](auto&& rec){
            write(fd, &rec, sizeof(rec));
        }, buffer[i]);
    }

    // log buffer를 초기화한다.
    buffer_tail = 0;

    // latch를 푼다.
    for (int i = 0; i < buffer_tail; ++i)
    {
        buffer_latch[i].unlock();
    }
}

LogManager::Message::Message(const std::string& logmsg_path)
    : fd(::open(logmsg_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666)),
      fp(fdopen(fd, "w+"))
{
    // Do nothing
}

void LogManager::open(const std::string& log_path, const std::string& logmsg_path)
{
    buffer.open(log_path);
    this->log_path = log_path;
    this->logmsg_path = logmsg_path;
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