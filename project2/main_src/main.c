#include <memory.h>
#include <stdio.h>
#include <stdlib.h>

#include "dbms.h"

int main()
{
    open_table("sample_100000.db");
    for (int i = 0; i < 100000; ++i)
    {
        if (db_insert(i, "sample_db") != 0)
        {
            printf("insert failed: %d\n", i);
            exit(-1);
        }
    }

    for (int i = 0; i < 100000; ++i)
    {
        char data[120];
        if (db_find(i, data) != 0)
        {
            printf("find failed: %d\n", i);
            exit(-1);
        }
    }

    for (int i = 100001; i < 100100; ++i)
    {
        char data[120];
        if (db_find(i, data) == 0)
        {
            printf("find failed: %d\n", i);
            exit(-1);
        }
    }

    puts("success");
    return 0;
}