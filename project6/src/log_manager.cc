#include "log_manager.hpp"
#include "scoped_page_latch.hpp"

#include <algorithm>


LogReader::LogReader(const std::string& log_path, int64_t start_lsn)
    : fd(open(log_path.c_str(), O_RDONLY)),
      now_lsn(start_lsn)
{
    // Do nothing
}

LogReader::LogReader(const std::string& log_path)
    : fd(open(log_path.c_str(), O_RDONLY))
{
    LogHeader header;
    header.header = 0;
    pread(fd, &header, sizeof(LogHeader), 0);
    // std::cout << result << "header: " << header.header << ",  " << header.start_lsn << std::endl;
    if (header.header == 0xffeeaaff)
    {
        now_lsn = header.start_lsn;
    }
    else
    {
        now_lsn = 0;
    }
    // std::cout << "now_lsn: " << now_lsn << std::endl;;
}


void LogReader::print() const
{
    int64_t now = 0;
    int readed = 1;

    do
    {
        LogType type;
        int32_t record_size;
        auto readed = pread(fd, &type, sizeof(type), now + 20);

         switch (type)
    {
        case LogType::BEGIN:
            record_size = sizeof(CommonLogRecord);
            break;
        case LogType::COMMIT:
            record_size = sizeof(CommonLogRecord);
            break;
        case LogType::COMPENSATE:
            record_size = sizeof(CompensateLogRecord);
            break;
        case LogType::ROLLBACK:
            record_size = sizeof(CommonLogRecord);
            break;
        case LogType::UPDATE:
            record_size = sizeof(UpdateLogRecord);
            break;
        case LogType::INVALID:
            record_size = 0;
            break;
    }

        if (!readed)
        {
            break;
        }

        switch (record_size)
        {
            case sizeof(CommonLogRecord): {
                CommonLogRecord rec;
                auto readed = pread(fd, &rec, sizeof(rec), now);
                now += readed;
                DB_CRASH_COND(readed == sizeof(rec), -1,
                              "read CommonLogRecord failure. pread return: %ld",
                              readed);
                std::cout << rec << "\n\n";
                break;
            }
            case sizeof(UpdateLogRecord): {
                UpdateLogRecord rec;
                auto readed = pread(fd, &rec, sizeof(rec), now);
                now += readed;
                DB_CRASH_COND(readed == sizeof(rec), -1,
                              "read UpdateLogRecord failure. pread return: %ld",
                              readed);
                std::cout << rec << "\n\n";
                break;
            }
            case sizeof(CompensateLogRecord): {
                CompensateLogRecord rec;
                auto readed = pread(fd, &rec, sizeof(rec), now);
                now += readed;
                DB_CRASH_COND(
                    readed == sizeof(rec), -1,
                    "read CompensateLogRecord failure. pread return: %ld",
                    readed);
                std::cout << rec << "\n\n";
                break;
            }
            default:
                DB_CRASH(-1, "invalid log record size: %d. lsn: %ld",
                         record_size, now);
        }
    } while (readed);
    
    std::cout << std::endl;
}

std::ostream& operator<<(std::ostream& os, const LogType& dt)
{
    switch (dt)
    {
        case LogType::BEGIN:
            os << "BEGIN";
            break;
        case LogType::UPDATE:
            os << "UPDATE";
            break;
        case LogType::COMMIT:
            os << "COMMIT";
            break;
        case LogType::ROLLBACK:
            os << "ROLLBACK";
            break;
        case LogType::COMPENSATE:
            os << "COMPENSATE";
            break;
        case LogType::INVALID:
            os << "INVALID";
            break;
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const CommonLogRecord& dt)
{
    os << dt.type << ": [" << dt.log_size << ", " << dt.lsn << ", "
       << dt.prev_lsn << ", " << dt.transaction_id << "]";
    return os;
}

std::ostream& operator<<(std::ostream& os, const UpdateLogRecord& dt)
{
    os << dt.type << ": [" << dt.log_size << ", " << dt.lsn << ", "
       << dt.prev_lsn << ", " << dt.transaction_id << ", " << dt.table_id
       << ", " << dt.page_number << ", " << dt.offset << ", " << dt.data_length
       << "]\n";
    os << "old_image: " << dt.old_image << "\nnew_image: " << dt.new_image;
    return os;
}

std::ostream& operator<<(std::ostream& os, const CompensateLogRecord& dt)
{
    os << dt.type << ": [" << dt.log_size << ", " << dt.lsn << ", "
       << dt.prev_lsn << ", " << dt.transaction_id << ", " << dt.table_id
       << ", " << dt.page_number << ", " << dt.offset << ", " << dt.data_length
       << "]\n";
    os << "old_image: " << dt.old_image << "\nnew_image: " << dt.new_image << "\nundo next: " << dt.next_undo_lsn;
    return os;
}

std::tuple<LogType, LogRecord> LogReader::get(int64_t lsn) const
{
    LogType type;
    int32_t record_size = 0;
    auto readed = pread(fd, &type, sizeof(type), lsn + 20);
    if (readed != sizeof(type))
    {
        // std::cout << lsn << ": invalid\n";
        return { LogType::INVALID, LogRecord() };
    }

    switch (type)
    {
        case LogType::BEGIN:
            record_size = sizeof(CommonLogRecord);
            break;
        case LogType::COMMIT:
            record_size = sizeof(CommonLogRecord);
            break;
        case LogType::COMPENSATE:
            record_size = sizeof(CompensateLogRecord);
            break;
        case LogType::ROLLBACK:
            record_size = sizeof(CommonLogRecord);
            break;
        case LogType::UPDATE:
            record_size = sizeof(UpdateLogRecord);
            break;
        case LogType::INVALID:
            record_size = 0;
            break;
        default:
            std::cout << "invalid type: " << int(type) << '\n';
            break; 
    }

    switch (record_size)
    {
        case sizeof(CommonLogRecord): {
            CommonLogRecord rec;
            auto readed = pread(fd, &rec, sizeof(rec), lsn);
            DB_CRASH_COND(readed == sizeof(rec), -1,
                          "read CommonLogRecord failure. pread return: %ld",
                          readed);
            LogType type = rec.type;
            return { type, rec };
        }
        case sizeof(UpdateLogRecord): {
            UpdateLogRecord rec;
            auto readed = pread(fd, &rec, sizeof(rec), lsn);
            DB_CRASH_COND(readed == sizeof(rec), -1,
                          "read UpdateLogRecord failure. pread return: %ld, "
                          "record size: ",
                          readed);
            LogType type = rec.type;
            return { type, rec };
        }
        case sizeof(CompensateLogRecord): {
            CompensateLogRecord rec;
            auto readed = pread(fd, &rec, sizeof(rec), lsn);
            DB_CRASH_COND(readed == sizeof(rec), -1,
                          "read CompensateLogRecord failure. pread return: %ld",
                          readed);
            LogType type = rec.type;
            return { type, rec };
        }
        default:
            DB_CRASH(-1, "invalid log record size: %d. lsn: %ld", record_size,
                     lsn);
    }
}

std::tuple<LogType, LogRecord> LogReader::next() const
{
    auto t = get(now_lsn);
    if (std::get<0>(t) != LogType::INVALID)
    {
        // std::cout << "now_lsn: " << now_lsn << ". plus "
        //           << get_log_record_size(std::get<1>(t)) << std::endl;
        now_lsn += get_log_record_size(std::get<1>(t));
    }
    return t;
}

std::tuple<LogType, LogRecord> LogReader::prev() const
{
    if (now_lsn == -1)
    {
        return { LogType::INVALID, LogRecord() };
    }
    auto t = get(now_lsn);
    if (std::get<0>(t) != LogType::INVALID)
    {
        int32_t record_size;
        auto readed = pread(fd, &record_size, sizeof(record_size), now_lsn - 4);
        if (readed != sizeof(record_size))
        {
            now_lsn = -1;
        }
        // std::cout << "now_lsn: " << now_lsn << ". minus " << record_size
        //           << std::endl;
        now_lsn -= record_size;
    }
    return t;
}

void LogReader::set_lsn(int64_t lsn)
{
    now_lsn = lsn;
}

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
        buffer_head = 0;
    }
}

void LogBuffer::truncate()
{
    LogHeader header;
    header.start_lsn = last_lsn;
    pwrite(fd, &header, sizeof(header), 0);
    // std::cout << "header seted: " << last_lsn << std::endl;
    
    fsync(fd);
    // ftruncate(fd, 0);
}

void LogBuffer::open(const std::string& log_path)
{
    fd = ::open(log_path.c_str(), O_WRONLY | O_CREAT, 0666);
}

int64_t LogBuffer::append(const LogRecord& record)
{
    std::unique_lock<std::mutex> value_latch_lock { value_latch };
    int my_lsn = last_lsn;
    last_lsn += get_log_record_size(record);
    int buffer_index = buffer_tail++;

    std::cout << "lsn: " << my_lsn << ", ";
    switch (record.index())
    {
        case 0:
            std::cout << std::get<CommonLogRecord>(record);
            break;
        case 1:
            std::cout << std::get<UpdateLogRecord>(record);
            break;
        case 2:
            std::cout << std::get<CompensateLogRecord>(record);
            break;
    }
    std::cout << std::endl;

    // TODO: buffer_index 넘치면 flush
    if (buffer_index == LOG_BUFFER_SIZE)
    {
        --buffer_tail;
        flush(true);

        buffer_index = buffer_tail++;
    }

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

void LogBuffer::flush(bool from_append)
{
    // flush는 한번에 한 스레드에서만 호출 가능하다.
    std::unique_lock<std::mutex> crit { flush_latch };

    // flush 중에는 log가 추가되는 것을 막는다
    // std::unique_lock<std::mutex> value_latch_lock { value_latch };
    std::unique_lock<std::mutex> value_latch_lock { value_latch,
                                                    std::defer_lock };

    // append 함수에서는 이미 value_latch를 잡고 있으므로, append 함수에서
    // 호출했을 때는 lock을 걸지 않는다.
    if (!from_append)
    {
        value_latch_lock.lock();
    }

    // 버퍼에 존재하는 모든 로그의 latch를 잠근다.
    // 로그를 작성하는 스레드늨 buffer_tail을 증가시킬 때 buffer_latch를 걸고,
    // 로그 작성이 완료된 후에 buffer_latch를 푼다. 즉, flush에서 모든
    // buffer_latch의 lock을 얻을 때까지 대기한다는 건, 현재 작성중인 모든
    // 로그의 작성이 완료될 때까지 대기한다는 의미이다.
    for (int i = buffer_head; i < buffer_tail; ++i)
    {
        buffer_latch[i].lock();
    }

    // O_APPEND 옵션이 있어 로그가 파일 맨 뒤에 작성되므로 flushed_lsn을
    // 신경쓰지 않아도 된다.
    for (int i = buffer_head; i < buffer_tail; ++i)
    {
        std::visit(
            [&](auto&& rec) {
                // std::cout << "flushed: " << rec << std::endl;
                pwrite(fd, &rec, sizeof(rec), rec.lsn);
            },
            buffer[i]);
    }

    // latch를 푼다.
    for (int i = buffer_head; i < buffer_tail; ++i)
    {
        buffer_latch[i].unlock();
    }

    // log buffer를 초기화한다.
    buffer_tail = 0;
    buffer_head = 0;

    // buffer 파일을 디스크에 기록한다.
    fsync(fd);
}

bool LogBuffer::flush_prev_lsn(int64_t page_lsn)
{
    // flush 함수는 한번에 한 스레드에서만 호출 가능하다.
    std::unique_lock<std::mutex> crit { flush_latch };

    // flush 중에는 log가 추가되는 것을 막는다
    std::unique_lock<std::mutex> value_latch_lock { value_latch };

    // buffer_latch를 잠그면서, page_lsn보다 큰 로그가 처음으로 등장하는 위치를
    // 찾는다.
    int border = buffer_head;
    for (border = buffer_head; border < buffer_tail; ++border)
    {
        buffer_latch[border].lock();
        if (std::visit(
                [&](auto&& arg) {
                    return arg.lsn > page_lsn;
                },
                buffer[border]))
        {
            buffer_latch[border].unlock();
            break;
        }
    }

    // O_APPEND 옵션이 있어 로그가 파일 맨 뒤에 작성되므로 flushed_lsn을
    // 신경쓰지 않아도 된다.
    for (int i = buffer_head; i < border; ++i)
    {
        std::visit(
            [&](auto&& rec) {
                write(fd, &rec, sizeof(rec));
            },
            buffer[i]);
    }

    // latch를 푼다.
    for (int i = buffer_head; i < border; ++i)
    {
        buffer_latch[i].unlock();
    }

    // buffer의 앞부분만 flush 했다.
    // 그러므로, buffer_head를 border 위치로 옮겨 앞부분F을 사용하지 않게
    // 만든다.

    buffer_head = border;

    // buffer 파일을 디스크에 기록한다.
    fsync(fd);

    return true;
}

void LogManager::open(const std::string& log_path,
                      const std::string& logmsg_path)
{
    std::cout << "log_path: " << log_path << std::endl << "logmsg_path: " << logmsg_path << std::endl;
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

void LogManager::reset()
{
    log_path.clear();
    logmsg_path.clear();
    buffer.reset();
    trx_table.clear();
    dirty_table.clear();
}

bool LogManager::recovery(RecoveryMode mode, int log_num)
{
    Message msg { logmsg_path };
    std::vector<int> winners;
    std::vector<int> losers;

    int64_t last_lsn = 0;

    // analysis
    {
        LogReader reader { log_path };
        reader.print();
        msg.analysis_start();

        while (true)
        {
            auto [type, rec] = reader.next();
            if (type == LogType::INVALID)
            {
                break;
            }

            last_lsn = get_log_record_lsn(rec);
            trx_table[get_log_record_trx(rec)] = last_lsn;

            if (type == LogType::BEGIN)
            {
                auto target = std::get<CommonLogRecord>(rec).transaction_id;
                auto it = std::find_if(winners.begin(), winners.end(),
                                       [target](int v) {
                                           return v == target;
                                       });
                if (it != winners.end())
                {
                    // 지워줌
                    winners.erase(it);
                }
                losers.emplace_back(target);
            }

            if (type == LogType::COMMIT || type == LogType::ROLLBACK)
            {
                auto target = std::get<CommonLogRecord>(rec).transaction_id;
                auto it =
                    std::find_if(losers.begin(), losers.end(), [target](int v) {
                        return v == target;
                    });
                losers.erase(it);
                winners.emplace_back(target);
            }

            if (type == LogType::UPDATE)
            {
                auto target = std::get<UpdateLogRecord>(rec);
                if (!BufferController::instance().fileManagerExist(
                        target.table_id))
                {
                    std::string s = "DATA" + std::to_string(target.table_id);
                    BufferController::instance().openFileManager(s);
                }
            }

            if (type == LogType::COMPENSATE)
            {
                auto target = std::get<CompensateLogRecord>(rec);
                if (!BufferController::instance().fileManagerExist(
                        target.table_id))
                {
                    std::string s = "DATA" + std::to_string(target.table_id);
                    BufferController::instance().openFileManager(s);
                }
            }
        }

        msg.analysis_end(winners, losers);
    }

    if (winners.empty() && losers.empty())
    {
        return true;
    }

    buffer.last_lsn = last_lsn;

    msg.redo_pass_start();

    // REDO
    {
        LogReader reader { log_path };
        bool flag = true;
        for (int i = 0; flag; ++i)
        {
            if (mode == RecoveryMode::REDO_CRASH && i == log_num)
            {
                // REDO CRASH
                exit(0);
            }

            auto [type, rec] = reader.next();
            switch (type)
            {
                case LogType::BEGIN: {
                    CommonLogRecord& record = std::get<CommonLogRecord>(rec);
                    msg.begin(record.lsn, record.transaction_id);
                    break;
                }
                case LogType::COMMIT: {
                    CommonLogRecord& record = std::get<CommonLogRecord>(rec);
                    msg.commit(record.lsn, record.transaction_id);
                    break;
                }
                case LogType::COMPENSATE: {
                    CompensateLogRecord& record =
                        std::get<CompensateLogRecord>(rec);

                    // redo compensate
                    page_t page;
                    BufferController::instance().get(record.table_id,
                                                     record.page_number, page);
                    // consider redo?
                    if (page.nodePageHeader().pageLsn < record.lsn)
                    {
                        std::memcpy((char*)(&page) + record.offset,
                                    (char*)(&record.new_image),
                                    record.data_length);
                        page.nodePageHeader().pageLsn = record.lsn;
                        BufferController::instance().put(
                            record.table_id, record.page_number, page);
                        msg.compensate(record.lsn, record.next_undo_lsn);
                    }
                    else
                    {
                        msg.consider_redo_update(record.lsn,
                                                 record.transaction_id);
                    }
                    break;
                }
                case LogType::ROLLBACK: {
                    CommonLogRecord& record = std::get<CommonLogRecord>(rec);
                    msg.commit(record.lsn, record.transaction_id);
                    break;
                }
                case LogType::UPDATE: {
                    UpdateLogRecord& record = std::get<UpdateLogRecord>(rec);

                    // redo update
                    page_t page;
                    BufferController::instance().get(record.table_id,
                                                     record.page_number, page);
                    // consider redo?
                    if (page.nodePageHeader().pageLsn < record.lsn)
                    {
                        std::memcpy((char*)(&page) + record.offset,
                                    (char*)(&record.new_image),
                                    record.data_length);
                        page.nodePageHeader().pageLsn = record.lsn;
                        BufferController::instance().put(
                            record.table_id, record.page_number, page);
                        msg.update_redo(record.lsn, record.transaction_id);
                    }
                    else
                    {
                        msg.consider_redo_update(record.lsn,
                                                 record.transaction_id);
                    }
                    break;
                }
                case LogType::INVALID:
                    flag = false;
                    break;
            }
        }
    }

    msg.redo_pass_end();
    msg.undo_pass_start();

    // UNDO
    {
        LogReader reader { log_path, last_lsn };

        bool flag = true;
        for (int i = 0; flag; ++i)
        {
            if (mode == RecoveryMode::UNDO_CRASH && i == log_num)
            {
                // REDO CRASH
                exit(0);
            }

            auto [type, rec] = reader.prev();
            if (type == LogType::INVALID)
            {
                break;
            }

            auto trx_id = get_log_record_trx(rec);
            auto it =
                std::find_if(losers.begin(), losers.end(), [trx_id](int v) {
                    return v == trx_id;
                });
            if (it == losers.end())
            {
                continue;
            }
            switch (type)
            {
                case LogType::BEGIN: {
                    CommonLogRecord& record = std::get<CommonLogRecord>(rec);
                    // msg.begin(record.lsn, record.transaction_id);
                    CommonLogRecord rec { INVALID_LSN,
                                          trx_table[record.transaction_id],
                                          record.transaction_id,
                                          LogType::ROLLBACK,
                                          sizeof(CommonLogRecord) };
                    trx_table[record.transaction_id] = buffer.append(rec);
                    break;
                }
                case LogType::COMMIT: {
                    // CommonLogRecord& record = std::get<CommonLogRecord>(rec);
                    // msg.commit(record.lsn, record.transaction_id);
                    break;
                }
                case LogType::COMPENSATE: {
                    CompensateLogRecord& record =
                        std::get<CompensateLogRecord>(rec);

                    // undo compensate
                    // page_t page;
                    // BufferController::instance().get(record.table_id,
                    //                                  record.page_number,
                    //                                  page);
                    // if (page.nodePageHeader().pageLsn < record.lsn)
                    {
                        // undo pass
                        // std::memcpy((char*)(&page) + record.offset,
                        //             (char*)(&record.old_image),
                        //             record.data_length);
                        // BufferController::instance().put(
                        //     record.table_id, record.page_number, page);
                        msg.compensate(record.lsn, record.next_undo_lsn);
                        reader.set_lsn(record.next_undo_lsn);
                    }
                    // else
                    // {
                    //     msg.consider_redo(record.lsn, record.transaction_id);
                    // }
                    break;
                }
                case LogType::ROLLBACK: {
                    // CommonLogRecord& record = std::get<CommonLogRecord>(rec);
                    // msg.commit(record.lsn, record.transaction_id);
                    break;
                }
                case LogType::UPDATE: {
                    UpdateLogRecord& record = std::get<UpdateLogRecord>(rec);

                    // redo update
                    page_t page;
                    BufferController::instance().get(record.table_id,
                                                     record.page_number, page);

                    CompensateLogRecord clr {
                        INVALID_LSN,           trx_table[record.transaction_id],
                        record.transaction_id, LogType::COMPENSATE,
                        record.table_id,       record.page_number,
                        record.offset,         record.data_length,
                        record.new_image,      record.old_image,
                        record.prev_lsn,       sizeof(CompensateLogRecord)
                    };
                    // std::cout << "clr: " << clr.next_undo_lsn << std::endl;
                    trx_table[record.transaction_id] = buffer.append(clr);
                    // if (page.nodePageHeader().pageLsn < record.lsn)
                    // {
                    std::memcpy((char*)(&page) + record.offset,
                                (char*)(&record.old_image), record.data_length);
                    BufferController::instance().put(record.table_id,
                                                     record.page_number, page);
                    msg.update_undo(record.lsn, record.transaction_id);
                    // }
                    // else
                    // {
                    //     msg.consider_redo(record.lsn, record.transaction_id);
                    // }
                    break;
                }
                case LogType::INVALID:
                    flag = false;
                    break;
            }
        }
    }

    msg.undo_pass_end();

    msg.flush_with_sync();
    buffer.flush();
    BufferController::instance().sync();

    buffer.truncate();

    return true;
}

int64_t LogManager::begin_log(int transaction_id)
{
    CommonLogRecord record { INVALID_LSN, INVALID_LSN, transaction_id,
                             LogType::BEGIN, sizeof(CommonLogRecord) };
    return log_wrapper(transaction_id, record);
}

int64_t LogManager::commit_log(int transaction_id)
{
    CommonLogRecord record { INVALID_LSN, trx_table[transaction_id],
                             transaction_id, LogType::COMMIT,
                             sizeof(CommonLogRecord) };
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
                             (int64_t)page_number,
                             offset,
                             data_length,
                             old_image,
                             new_image,
                             sizeof(UpdateLogRecord) };
    return log_wrapper(transaction_id, record);
}

bool LogManager::flush_prev_lsn(int64_t page_lsn)
{
    return buffer.flush_prev_lsn(page_lsn);
}

bool LogManager::flush()
{
    buffer.flush();
    return true;
}

bool LogManager::rollback(int transaction_id)
{
    std::cout << "rollback\n";
    auto it = trx_table.find(transaction_id);
    if (it == trx_table.end())
    {
        // 트랜잭션이 begin 할때 무조건 begin 로그가 작성되는데, trx_table에 trx
        // id가 없다는 건 trx id가 invalid 하다는 의미
        std::cout << "???\n";
        return false;
    }

    int now = it->second;

    // rollback 전에 buffer를 flush 하면, abort된 트랜잭션의 로그가 buffer에
    // 일부 남아있는 케이스를 무시하고 간단하게 구현할 수 있다.
    buffer.flush();

    LogReader reader { log_path };
    // reader.print();

    while (now != INVALID_LSN)
    {
        auto [type, rec] = reader.get(now);
        switch (type)
        {
            case LogType::BEGIN:
                // now가 INVALID_LSN이여만 한다.
                now = std::get<CommonLogRecord>(rec).prev_lsn;
                std::cout << "rollback: " << std::get<CommonLogRecord>(rec) << std::endl;
                CHECK(now == INVALID_LSN);
                break;

            case LogType::UPDATE: {
                auto& record = std::get<UpdateLogRecord>(rec);

                 std::cout << "rollback: " << record << std::endl;
                // TODO: table_id와 file_id가 같은가?
                // compensate 로그를 먼저 쓴다.
                CompensateLogRecord clr {
                    INVALID_LSN,      trx_table[transaction_id],
                    transaction_id,   LogType::COMPENSATE,
                    record.table_id,  record.page_number,
                    record.offset,    record.data_length,
                    record.new_image, record.old_image,
                    record.prev_lsn,  sizeof(CompensateLogRecord)
                };

                auto lsn = buffer.append(clr);

                scoped_node_latch latch { record.table_id, (pagenum_t)record.page_number };

                {
                    std::unique_lock<std::mutex> trx_table_latch_lock {
                        trx_table_latch
                    };
                    trx_table[transaction_id] = lsn;
                }

                page_t page;
                BufferController::instance().get(record.table_id,
                                                 record.page_number, page);
                page.nodePageHeader().pageLsn = lsn;
                std::memcpy((char*)(&page) + record.offset,
                            (char*)(&record.old_image), record.data_length);
                BufferController::instance().put(record.table_id,
                                                 record.page_number, page);
                now = record.prev_lsn;
                break;
            }

            case LogType::COMPENSATE:
                // 트랜잭션이 abort 될 때, compensate 로그가 있으면 안된다.
                std::cerr << "compensate log cannot be rolled back";
                exit(-1);
                break;

            case LogType::ROLLBACK:
                // rollback에 성공한 trx를 또 rollback 하는건 논리적 오류이다!
                std::cerr
                    << "rolled back transaction cannot be rolled back again";
                exit(-1);
                break;

            case LogType::COMMIT:
                // commit에 성공한 trx를 rollback 하는건 논리적 오류이다!
                std::cerr << "commited transaction cannot be rolled back";
                exit(-1);
                break;

            case LogType::INVALID:
                // invalid가 등장하면 안된다
                std::cerr << "invalid log type";
                exit(-1);
                break;
        }
    }

    CommonLogRecord record { INVALID_LSN, trx_table[transaction_id],
                             transaction_id, LogType::ROLLBACK,
                             sizeof(CommonLogRecord) };
    buffer.append(record);

    {
        std::unique_lock<std::mutex> trx_table_latch_lock { trx_table_latch };
        trx_table.erase(transaction_id);
    }

    buffer.flush();

    return true;
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
    fprintf(stdout, "[ANALYSIS] Analysis pass start\n");
}

inline void LogManager::Message::analysis_end(const std::vector<int>& winners,
                                              const std::vector<int>& losers)
{
    fprintf(fp, "[ANALYSIS] Analysis success. Winner:");
    fprintf(stdout, "[ANALYSIS] Analysis success. Winner:");
    for (auto& i : winners)
    {
        fprintf(fp, " %d", i);
        fprintf(stdout, " %d", i);
    }
    fprintf(fp, ", Loser:");
    fprintf(stdout, ", Loser:");
    for (auto& i : losers)
    {
        fprintf(fp, " %d", i);
        fprintf(stdout, " %d", i);
    }
    fprintf(fp, "\n");
    fprintf(stdout, "\n");
}

inline void LogManager::Message::redo_pass_start()
{
    fprintf(fp, "[REDO] Redo pass start\n");
    printf("[REDO] Redo pass start\n");
}

inline void LogManager::Message::undo_pass_start()
{
    fprintf(fp, "[UNDO] Undo pass start\n");
    printf("[UNDO] Undo pass start\n");
}

inline void LogManager::Message::begin(int64_t lsn, int trx_id)
{
    fprintf(fp, "LSN %lu[BEGIN] Transaction id %d\n",
            lsn + sizeof(CommonLogRecord), trx_id);
    printf("LSN %lu[BEGIN] Transaction id %d\n", lsn + sizeof(CommonLogRecord),
           trx_id);
}

inline void LogManager::Message::update_redo(int64_t lsn, int trx_id)
{
    fprintf(fp, "LSN %lu[UPDATE] Transaction id %d redo apply\n",
            lsn + sizeof(UpdateLogRecord), trx_id);
    fprintf(stdout, "LSN %lu[UPDATE] Transaction id %d redo apply\n",
            lsn + sizeof(UpdateLogRecord), trx_id);
}

inline void LogManager::Message::update_undo(int64_t lsn, int trx_id)
{
    fprintf(fp, "LSN %lu[UPDATE] Transaction id %d undo apply\n",
            lsn + sizeof(UpdateLogRecord), trx_id);
    fprintf(stdout, "LSN %lu[UPDATE] Transaction id %d undo apply\n",
            lsn + sizeof(UpdateLogRecord), trx_id);
}

inline void LogManager::Message::commit(int64_t lsn, int trx_id)
{
    fprintf(fp, "LSN %lu[COMMIT] Transaction id %d\n",
            lsn + sizeof(CommonLogRecord), trx_id);
    fprintf(stdout, "LSN %lu[COMMIT] Transaction id %d\n",
            lsn + sizeof(CommonLogRecord), trx_id);
}

inline void LogManager::Message::rollback(int64_t lsn, int trx_id)
{
    fprintf(fp, "LSN %lu[ROLLBACK] Transaction id %d\n",
            lsn + sizeof(CommonLogRecord), trx_id);
    fprintf(stdout, "LSN %lu[ROLLBACK] Transaction id %d\n",
            lsn + sizeof(CommonLogRecord), trx_id);
}

inline void LogManager::Message::compensate(int64_t lsn, int64_t next_undo_lsn)
{
    fprintf(fp, "LSN %lu[CLR] next undo lsn %lu\n",
            lsn + sizeof(CompensateLogRecord), next_undo_lsn);
    fprintf(stdout, "LSN %lu[CLR] next undo lsn %lu\n",
            lsn + sizeof(CompensateLogRecord), next_undo_lsn);
}

inline void LogManager::Message::consider_redo_update(int64_t lsn, int trx_id)
{
    fprintf(fp, "LSN %lu[CONSIDER - REDO] Transaction id %d\n",
            lsn + sizeof(UpdateLogRecord), trx_id);
    fprintf(stdout, "LSN %lu[CONSIDER - REDO] Transaction id %d\n",
            lsn + sizeof(UpdateLogRecord), trx_id);
}

inline void LogManager::Message::consider_redo_compensate(int64_t lsn,
                                                          int trx_id)
{
    fprintf(fp, "LSN %lu[CONSIDER - REDO] Transaction id %d\n",
            lsn + sizeof(CompensateLogRecord), trx_id);
    fprintf(stdout, "LSN %lu[CONSIDER - REDO] Transaction id %d\n",
            lsn + sizeof(CompensateLogRecord), trx_id);
}

inline void LogManager::Message::redo_pass_end()
{
    fprintf(fp, "[REDO] Redo pass end\n");
    fprintf(stdout, "[REDO] Redo pass end\n");
}

inline void LogManager::Message::undo_pass_end()
{
    fprintf(fp, "[UNDO] Undo pass end\n");
    fprintf(stdout, "[UNDO] Undo pass end\n");
}

inline void LogManager::Message::flush_with_sync()
{
    std::fflush(fp);
    fsync(fd);
}