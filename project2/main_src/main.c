#include <stdio.h>

#include "dbms.h"
#include "dbms.h"

#include <memory.h>

int main()
{
    open_table("sample_100000.db");
    for (int i = 0; i < 100000; ++i)
    {
        db_insert(i, "sample_db");
    }

    for (int i = 0; i < 100000; ++i)
    {
        char data[120];
        memset(data, 65, 120);
        db_find(i, data);
        puts(data);
    }
    return 0;
}