#include "bpt_api.h"

#include <cstdio>

extern "C"
int open_table(char* pathname)
{
    puts("open_table");
}

extern "C"
int db_insert(int64_t key, char* value)
{
    puts("db_insert");
}

extern "C"
int db_find(int64_t key, char* ret_val)
{
    puts("db_find");
}

extern "C"
int db_delete(int64_t key)
{
    puts("db_find");
}