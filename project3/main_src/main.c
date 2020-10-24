#include <memory.h>
#include <stdio.h>
#include <stdlib.h>

#include "dbms.h"

int main()
{
    // open_table("sample.db");
    // for (int j = 0; j < 2; ++j)
    // {
    //     for (int i = 0; i < 10000000; ++i)
    //     {
    //         if (db_insert(i, "sample_db") != 0)
    //         {
    //             printf("insert failed: %d\n", i);
    //             exit(-1);
    //         }
    //     }

    //     for (int i = 0; i < 10000000; ++i)
    //     {
    //         char data[120];
    //         if (db_find(i, data) != 0)
    //         {
    //             printf("find failed: %d\n", i);
    //             exit(-1);
    //         }
    //     }

    //     for (int i = 10000001; i < 10000100; ++i)
    //     {
    //         char data[120];
    //         if (db_find(i, data) == 0)
    //         {
    //             printf("find failed: %d\n", i);
    //             exit(-1);
    //         }
    //     }

    //     for (int i = 0; i < 5000000; ++i)
    //     {
    //         if (db_delete(i) != 0)
    //         {
    //             printf("delete failed: %d\n", i);
    //             exit(-1);
    //         }
    //     }

    //     for (int i = 5000001; i < 10000000; ++i)
    //     {
    //         char data[120];
    //         if (db_find(i, data) != 0)
    //         {
    //             printf("find after delete failed: %d\n", i);
    //             exit(-1);
    //         }
    //     }

    //     for (int i = 0; i < 5000000; ++i)
    //     {
    //         char data[120];
    //         if (db_find(i, data) == 0 || db_delete(i) == 0)
    //         {
    //             printf("find deleted: %d\n", i);
    //             exit(-1);
    //         }
    //     }

    //     for (int i = 9999999; i >= 5000000; --i)
    //     {
    //         if (db_delete(i) != 0)
    //         {
    //             printf("delete2 failed: %d\n", i);
    //             exit(-1);
    //         }
    //     }

    //     for (int i = 9999999; i >= 0; --i)
    //     {
    //         char data[120];
    //         if (db_find(i, data) == 0 || db_delete(i) == 0)
    //         {
    //             printf("find deleted2: %d\n", i);
    //             exit(-1);
    //         }
    //     }
    // }
    // puts("success");
    return 0;
}