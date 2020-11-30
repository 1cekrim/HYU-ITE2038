#include <memory.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "dbms.h"

#define CHECK_SUCCESS(expression)            \
    if ((expression))                        \
    {                                        \
        printf("[(" #expression " != 0]\n"); \
        return -1;                           \
    }

#define CHECK_FAILURE(expression)            \
    if (!(expression))                       \
    {                                        \
        printf("[(" #expression " == 0]\n"); \
        return -1;                           \
    }

#define DO_TEST(expression)     \
    if ((expression))           \
    {                           \
        puts("[TEST SUCCESS]"); \
    }                           \
    else                        \
    {                           \
        puts("[TEST FAILED]");  \
    }

// num_records개의 레코드가 있다.
// 여기에 0을 insert 한다.
// 각 thread는 레코드를 read 하고, 1을 더한다음, commit 하는 걸 반복한다.
// 실패할 경우 다시 시도한다.
// 모든 스레드가 종료된 이후, 모든 record의 값이 repeat * num_thread인지
// 확인한다.

struct arg
{
    int* tables;
    int num_records;
    int num_tables;
    int repeat;
};

int test(int num_thread, int tables, int num_records, int repeat);

void* thread_func(void* a);

int main()
{
    test(10, 10, 100, 100000);
}

int test(int num_thread, int num_tables, int num_records, int repeat)
{
    printf(
        "[TEST] num_thread: %d / num_tables: %d / num_records: %d / repeat: "
        "%d\n",
        num_thread, num_tables, num_records, repeat);
    printf("init db...");
    CHECK_SUCCESS(init_db(1000000));

    int tables[10];

    printf("open %d tables... ", num_tables);
    fflush(stdout);
    {
        for (int j = 0; j < num_tables; ++j)
        {
            char filename[20];
            sprintf(filename, "table_%d.db", j);

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

    printf("insert %d zeros to %d tables... ", num_records, num_tables);
    fflush(stdout);
    {
        for (int j = 0; j < num_records; ++j)
        {
            for (int k = 0; k < num_tables; ++k)
            {
                char val[120] = "0";
                int key = j;
                CHECK_SUCCESS(db_insert(tables[k], key, val));
            }
        }
    }
    puts("success");

    printf("find %d zeros to %d tables... ", num_records, num_tables);
    fflush(stdout);
    {
        for (int j = 0; j < num_records; ++j)
        {
            for (int k = 0; k < num_tables; ++k)
            {
                char ans[120];
                int key = j;

                CHECK_SUCCESS(db_find(tables[k], key, ans, 0));
                CHECK_FAILURE(atoi(ans) == 0)
            }
        }
    }
    puts("success");

    pthread_t* threads = (pthread_t*)malloc(sizeof(pthread_t) * num_thread);

    printf("thread...");
    struct arg a = { tables, num_records, num_tables, repeat };

    fflush(stdout);
    {
        for (int i = 0; i < num_thread; ++i)
        {
            pthread_create(&threads[i], NULL, thread_func, (void*)&a);
        }

        for (int i = 0; i < num_thread; ++i)
        {
            pthread_join(threads[i], NULL);
        }
    }
    puts("success");

    printf("result check...");
    fflush(stdout);
    {
        for (int j = 0; j < num_records; ++j)
        {
            for (int k = 0; k < num_tables; ++k)
            {
                char ans[120];
                int key = j;

                CHECK_SUCCESS(db_find(tables[k], key, ans, 0));
                CHECK_FAILURE(atoi(ans) == repeat * num_thread);
            }
        }
    }
    puts("success");
}

void* thread_func(void* a)
{
    struct arg* b = a;
    int* tables = b->tables;
    int num_records = b->num_records;
    int num_tables = b->num_tables;
    int repeat = b->repeat;
    int* numbers = (int*)malloc(sizeof(int) * num_records);
    memset(numbers, 0, sizeof(sizeof(int) * num_records));

    printf("make %d random numbers... ", num_records);
    fflush(stdout);
    {
        for (int j = 1; j <= num_records; ++j)
        {
            int pos = rand() % j;
            numbers[j - 1] = numbers[pos];
            numbers[pos] = j - 1;
        }
    }
    puts("success");
    for (int q = 0; q < 100000; ++q)
    {

    for (int i = 0; i < num_tables; ++i)
    {
        for (int k = 0; k < num_records; ++k)
        {
            int abort = 1;
            while (abort)
            {
                abort = 0;
                int trx = trx_begin();
                for (int j = 0; j < repeat; ++j)
                {
                    char ans[120];
                    int key = numbers[k];

                    if (db_find(tables[i], key, ans, trx))
                    {
                        abort = 1;
                        break;
                    }
                    

                    // int before = atoi(ans);
                    // int num = atoi(ans) + 1;
                    // sprintf(ans, "%d", num);
                    // if (db_update(tables[i], key, ans, trx))
                    // {
                    //     abort = 1;
                    //     break;
                    // }

                    // if (db_find(tables[i], key, ans, trx))
                    // {
                    //     abort = 1;
                    //     break;
                    // }
                    // int after = atoi(ans);
                }
                if (!abort)
                {
                    trx_commit(trx);
                }
            }
        }
    }
    }
}