#include "dbms_api.hpp"

#include <cstdio>

extern "C" {
int open_table(char* pathname)
{
    puts("open_table");
}

int db_insert(int64_t key, char* value)
{
    puts("db_insert");
}

int db_find(int64_t key, char* ret_val)
{
    puts("db_find");
}

int db_delete(int64_t key)
{
    puts("db_find");
}
}
