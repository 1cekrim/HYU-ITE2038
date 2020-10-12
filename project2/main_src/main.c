#include <stdio.h>

#include "dbms.h"
#include "dbms.h"

int main()
{
    open_table("sample_10000.db");
    for (int i = 1; i < 10000; ++i)
    {
        char data[120];
        db_find(i, data);
        puts(data);
    }
    return 0;
}