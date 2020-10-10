#include <stdio.h>

#include "dbms.h"
#include "file.h"

int main()
{
    if (open_table("main.db") == -1)
    {
        puts("open table error");
        return -1;
    }

    db_insert(0, "hello 0");
    db_insert(2, "hello 2");
    db_insert(1, "hello 1");
    db_insert(4, "hello 4");
    db_insert(3, "hello 3");
    db_insert(5, "hello 5");
    db_insert(8, "hello 8");
    db_insert(7, "hello 7");
    db_insert(6, "hello 6");

    for (int i = 0; i <= 8; ++i)
    {
        char ret[120];
        db_find(i, ret);
        printf("%s\n", ret);
    }

    db_delete(0);
    return 0;
}