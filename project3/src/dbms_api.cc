#include "dbms_api.hpp"

#include <cstdio>

#include "table_manager.hpp"

extern "C" {

int init_db(int buf_num)
{
    return TableManager::instance().init_db(buf_num) ? 0 : -1;
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

int db_find(int table_id, int64_t key, char* ret_val)
{
    record_t record;
    return TableManager::instance().find(table_id, key, record) ? 0 : -1;
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
}
