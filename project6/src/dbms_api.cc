#include "dbms_api.hpp"

#include <cstdio>

#include "lock_manager.hpp"
#include "table_manager.hpp"
#include "transaction_manager.hpp"
#include "log_manager.hpp"

int init_db(int buf_num)
{
    return init_db(buf_num, 0, 0, (char*)"default.log", (char*)"msg.txt");
}

int init_db(int buf_num, int flag, int log_num, char* log_path, char* logmsg)
{
    // TODO: 구현
    LockManager::instance().reset();
    TransactionManager::instance().reset();
    LogManager::instance().reset();
    return TableManager::instance().init_db(buf_num, flag, log_num, log_path, logmsg) ? 0 : -1;
}

int open_table(char* pathname)
{
    int table_id = TableManager::instance().open_table(pathname);
    return table_id != INVALID_TABLE_ID ? table_id : -1;
}

int db_insert(int table_id, int64_t key, char* value)
{
    valType val;
    TableManager::char_to_valType(val, value);
    return TableManager::instance().insert(table_id, key, val) ? 0 : -1;
}

int db_find(int table_id, int64_t key, char* ret_val, int trx_id)
{
    record_t record;
    if (!TableManager::instance().find(table_id, key, record, trx_id))
    {
        return -1;
    }

    for (int i = 0; i < value_size; ++i)
    {
        if (!record.value[i])
        {
            ret_val[i] = '\0';
            break;
        }
        ret_val[i] = record.value[i];
    }

    return 0;
}

int db_update(int table_id, int64_t key, char* values, int trx_id)
{
    record_t record;
    // if (!TableManager::instance().find(table_id, key, record))
    // {
    //     return -1;
    // }

    TableManager::char_to_valType(record.value, values);
    return TableManager::instance().update(table_id, key, record.value, trx_id)
               ? 0
               : 1;
}

int trx_abort(int trx_id)
{
    std::unique_lock<std::mutex> buffer_latch {
        BufferController::instance().mtx
    };
    std::unique_lock<std::mutex> trxmanager_latch {
        TransactionManager::instance().mtx
    };
    return TransactionManager::instance().abort(trx_id);
}

int db_delete(int table_id, int64_t key)
{
    return TableManager::instance().delete_key(table_id, key) ? 0 : -1;
}

int close_table(int table_id)
{
    return TableManager::instance().close_table(table_id) ? 0 : -1;
}

int shutdown_db()
{
    return TableManager::instance().shutdown_db() ? 0 : -1;
}

int trx_begin()
{
    return TransactionManager::instance().begin();
}

int trx_commit(int trx_id)
{
    return TransactionManager::instance().commit(trx_id);
}