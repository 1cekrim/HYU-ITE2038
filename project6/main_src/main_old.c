// #include <memory.h>
// #include <stdio.h>
// #include <stdlib.h>
// #include <string.h>
// #include <time.h>

// #include "dbms.h"

// int test(int, int, int, int, int);

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

// int main()
// {
//     const int small_buffer = 10;
//     const int middle_buffer = 10000;
//     const int big_buffer = 1000000;
//     const int middle_records = 10000;
//     const int big_records = 1000000;

//     srand(time(NULL));

//     puts("open 10 table with middle buffer");
//     DO_TEST(test(middle_buffer, middle_records, 3, 10, 0));

//     puts("open 10 table with small buffer");
//     DO_TEST(test(small_buffer, middle_records, 3, 10, 1));

//     puts("open 10 table with big buffer");
//     DO_TEST(test(big_buffer, middle_records, 3, 10, 2));

//     puts("insert 1,000,000 test with small buffer");
//     DO_TEST(test(small_buffer, big_records, 1, 1, 3));

//     return 0;
// }

// int test(int buffer_size, int num_records, int repeat, int table_num,
//          int test_index)
// {
//     printf(
//         "\n[TEST %d START]\nbuffer_size: %d\nnum_records: %d\nrepeat: "
//         "%d\ntable_num: %d\n",
//         test_index, buffer_size, num_records, repeat, table_num);
//     printf("init db... ");
//     fflush(stdout);
//     {
//         CHECK_SUCCESS(init_db(buffer_size));
//     }
//     puts("success");

//     for (int i = 0; i < repeat; ++i)
//     {
//         printf("\nrepeat test %d\n", i);
//         int tables[10];
//         int* numbers = (int*)malloc(sizeof(int) * num_records);
//         memset(numbers, 0, sizeof(sizeof(int) * num_records) + 1);

//         printf("make %d random numbers... ", num_records);
//         fflush(stdout);
//         {
//             for (int j = 1; j <= num_records; ++j)
//             {
//                 int pos = rand() % j;
//                 numbers[j - 1] = numbers[pos];
//                 numbers[pos] = j;
//             }
//         }
//         puts("success");

//         printf("open %d tables... ", table_num);
//         fflush(stdout);
//         {
//             for (int j = 0; j < table_num; ++j)
//             {
//                 char filename[20];
//                 sprintf(filename, "table_%d_%d.db", test_index, j);

//                 int id = open_table(filename);
//                 if (id < 1 || id > 10)
//                 {
//                     printf("open failure. id: %d", id);
//                     return -1;
//                 }
//                 tables[j] = id;
//             }
//         }
//         puts("success");

//         printf("insert %d records to %d tables... ", num_records, table_num);
//         fflush(stdout);
//         {
//             for (int j = 0; j < num_records; ++j)
//             {
//                 for (int k = 0; k < table_num; ++k)
//                 {
//                     char val[120];
//                     int key = numbers[j] + k * num_records;
//                     sprintf(val, "test insert %d", key);
//                     CHECK_SUCCESS(db_insert(tables[k], key, val));
//                 }
//             }
//         }
//         puts("success");

//         printf("find %d records to %d tables... ", num_records, table_num);
//         fflush(stdout);
//         {
//             for (int j = 0; j < num_records; ++j)
//             {
//                 for (int k = 0; k < table_num; ++k)
//                 {
//                     char val[120];
//                     char ans[120];
//                     int key = numbers[j] + k * num_records;
//                     sprintf(ans, "test insert %d", key);

//                     CHECK_SUCCESS(db_find(tables[k], key, val));
//                     CHECK_SUCCESS(strcmp(val, ans));
//                 }
//             }
//         }
//         puts("success");

//         if (table_num == 10)
//         {
//             puts("open 11 tables failure... ");
//             {
//                 CHECK_FAILURE(open_table("invalid_table_name.db"));
//             }
//         }

//         printf("close %d tables... ", table_num);
//         fflush(stdout);
//         {
//             for (int k = 0; k < table_num; ++k)
//             {
//                 CHECK_SUCCESS(close_table(tables[k]));
//             }
//         }
//         puts("success");

//         printf("reopen %d tables... ", table_num);
//         fflush(stdout);
//         {
//             for (int j = 0; j < table_num; ++j)
//             {
//                 char filename[20];
//                 sprintf(filename, "table_%d_%d.db", test_index, j);

//                 int id = open_table(filename);
//                 if (id < 1 || id > 10)
//                 {
//                     printf("open failure. id: %d", id);
//                     return -1;
//                 }
//                 tables[j] = id;
//             }
//         }
//         puts("success");

//         printf("delete and insert %d records to %d tables... ", num_records,
//                table_num);
//         fflush(stdout);
//         {
//             for (int j = 0; j < num_records; ++j)
//             {
//                 for (int k = 0; k < table_num; ++k)
//                 {
//                     char val[120];
//                     int key = numbers[j] + k * num_records;
//                     sprintf(val, "test insert %d", key);

//                     char ret_val[120];

//                     CHECK_SUCCESS(db_delete(tables[k], key));
//                     CHECK_FAILURE(db_find(tables[k], key, ret_val));
//                     CHECK_SUCCESS(db_insert(tables[k], key, val));
//                     CHECK_SUCCESS(db_find(tables[k], key, ret_val));
//                     CHECK_SUCCESS(strcmp(val, ret_val));
//                 }
//             }
//         }
//         puts("success");

//         printf("delete all %d records to %d tables... ", num_records,
//                table_num);
//         fflush(stdout);
//         {
//             for (int j = 0; j < num_records; ++j)
//             {
//                 for (int k = 0; k < table_num; ++k)
//                 {
//                     int key = numbers[j] + k * num_records;
//                     char ret_val[120];

//                     CHECK_SUCCESS(db_delete(tables[k], key));
//                     CHECK_FAILURE(db_find(tables[k], key, ret_val));
//                 }
//             }
//         }
//         puts("success");

//         free(numbers);
//     }

//     printf("shutdown db... ");
//     fflush(stdout);
//     {
//         CHECK_SUCCESS(shutdown_db());
//     }
//     puts("success");

//     printf("use table after shutdown... ");
//     fflush(stdout);
//     {
//         char val[120];
//         CHECK_FAILURE(close_table(1));
//         CHECK_FAILURE(db_delete(1, 1));
//         CHECK_FAILURE(db_find(1, 1, val));
//         CHECK_FAILURE(db_insert(1, 1, val));
//         int id = open_table("after_shutdown.db");
//         if (id >= 1 && id <= 10)
//         {
//             printf("open success. id: %d", id);
//             return -1;
//         }
//     }
//     puts("success");
// }