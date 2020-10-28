#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "dbms.h"

int test(int, int, int, int, int);

#define CHECK_SUCCESS(expression)            \
    if ((expression))                        \
    {                                        \
        printf("[(" #expression " != 0]\n"); \
        return -1;                           \
    }

int main()
{
    const int small_buffer = 10;
    const int middle_buffer = 10000;
    const int big_buffer = 1000000;
    const int middle_records = 10000;

    srand(time(NULL));

    puts("open 10 table with middle buffer");
    test(middle_buffer, middle_records, 3, 10, 0);

    return 0;
}

int test(int buffer_size, int num_records, int repeat, int table_num,
         int test_index)
{
    printf(
        "[TEST %d START]\nbuffer_size: %d\nnum_records: %d\nrepeat: "
        "%d\ntable_num: %d\n",
        test_index, buffer_size, num_records, repeat, table_num);
    printf("init db... ");
    fflush(stdout);
    {
        CHECK_SUCCESS(init_db(buffer_size));
    }
    puts("success");

    for (int i = 0; i < repeat; ++i)
    {
        printf("repeat test %d\n", i);
        int tables[10];
        int* numbers = (int*)malloc(sizeof(int) * num_records);
        memset(numbers, 0, sizeof(sizeof(int) * num_records) + 1);

        printf("make %d random numbers... ", num_records);
        fflush(stdout);
        {
            for (int j = 1; j <= num_records; ++j)
            {
                int pos = rand() % j;
                numbers[j - 1] = numbers[pos];
                numbers[pos] = j;
            }
        }
        puts("success");

        printf("open %d tables... ", table_num);
        fflush(stdout);
        {
            for (int j = 0; j < table_num; ++j)
            {
                char filename[20];
                sprintf(filename, "table_%d_%d.db", test_index, j);

                int id = open_table(filename);
                if (id < 1 || id > 10)
                {
                    printf("open failure. id: %d", id);
                    return -1;
                }
                tables[j] = id;
            }
        }
        puts("success");

        free(numbers);
    }

    puts("shutdown db... ");
    {
        CHECK_SUCCESS(shutdown_db());
    }
    puts("success");
}