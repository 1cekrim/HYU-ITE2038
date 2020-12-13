#include "test.hpp"

#include <chrono>
#include <cstdio>
#include <future>
#include <iostream>
#include <list>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "bptree.hpp"
#include "file_api.hpp"
#include "file_manager.hpp"
#include "lock_manager.hpp"
#include "log_manager.hpp"
#include "logger.hpp"
#include "table_manager.hpp"
#include "transaction_manager.hpp"

int success, cnt, allSuccess, allCnt;
bool testFlag;
std::string testName;

void TEST_FILE_MANAGER();
void TEST_POD();
void TEST_BPT();
void TEST_FILE();
void TESTS();
void TEST_TABLE();
// void TEST_MULTITHREADING();
// void TEST_LOCK();
void TEST_TRANSACTION();
void TEST_LOG();

int main()
{
    // void (*tests[])() = {
    //     TEST_POD, TEST_FILE_MANAGER, TEST_TABLE, TEST_BPT,
    //     TEST_FILE,           TESTS
    // };

    // std::string testNames[] = {
    //     "test pod", "file_manager", "table", "bpt",
    //     "test_file",           "TESTS"
    // };

    void (*tests[])() = { TEST_LOG };

    std::string testNames[] = { "test log" };

    for (int i = 0;
         i < static_cast<int>((sizeof(tests) / sizeof(void (*)(void)))); ++i)
    {
        success = cnt = 0;

        std::cout << "[" << i << "| " << testNames[i] << " START]\n";
        tests[i]();
        std::cout << "[success: " << success << " / " << cnt << "]\n[" << i
                  << "| " << testNames[i] << " END]\n\n";
    }

    std::cout << "\n[Tests are over] success: " << allSuccess << " / " << allCnt
              << "\n";
    return 0;
}

void TEST_LOG()
{
    TEST("log record size")
    {
        CHECK_VALUE(sizeof(CommonLogRecord), 28);
        CHECK_VALUE(sizeof(UpdateLogRecord), 288);
        CHECK_VALUE(sizeof(CompensateLogRecord), 296);
    }
    END()

    // TEST("log buffer")
    // {
    //     LogBuffer buffer;
    // }
    // END()
    // TEST("begin commit")
    // {
    //     LogManagerLegacy::instance().reset();
    //     LockManager::instance().reset();
    //     TransactionManager::instance().reset();

    //     int trans_id1 = TransactionManager::instance().begin();
    //     CHECK_TRUE(TransactionManager::instance().commit(trans_id1));
    //     auto log = LogManagerLegacy::instance().trace_log(trans_id1);
    //     CHECK_VALUE(log.size(), 2);
    //     CHECK_TRUE(log[0].transaction == trans_id1);
    //     CHECK_TRUE(log[0].type == LogTypeLegacy::COMMIT);
    //     CHECK_TRUE(log[1].transaction == trans_id1);
    //     CHECK_TRUE(log[1].type == LogTypeLegacy::BEGIN);
    // }
    // END()

    TEST("rollback test")
    {
        LogManager::instance().reset();
        LockManager::instance().reset();
        TransactionManager::instance().reset();

        TableManager::instance().init_db(100, 0, 0, (char*)"rollback.log",
                                         (char*)"rollback.txt");
        auto id = TableManager::instance().open_table("rollback.db");

        constexpr auto num_records = 10000;

        for (int i = 0; i < num_records; ++i)
        {
            valType v;
            std::stringstream ss;
            ss << "test insert " << i;
            TableManager::char_to_valType(v, ss.str().c_str());
            CHECK_TRUE(TableManager::instance().insert(id, i, v));
        }

        auto trx = TransactionManager::instance().begin();
        for (int i = 0; i < num_records; ++i)
        {
            std::stringstream ss;
            ss << "test insert " << i;
            record_t record;
            CHECK_TRUE(TableManager::instance().find(id, i, record, trx));
            CHECK_VALUE(record.key, i);
            std::string result(std::begin(record.value),
                               std::end(record.value));
            result =
                std::string(std::begin(result),
                            std::begin(result) + std::strlen(result.c_str()));
            CHECK_VALUE(ss.str(), result);
        }

        for (int i = 0; i < num_records; ++i)
        {
            valType v;
            std::stringstream ss;
            ss << "test update " << i;
            TableManager::char_to_valType(v, ss.str().c_str());
            CHECK_TRUE(TableManager::instance().update(id, i, v, trx));
        }

        for (int i = 0; i < num_records; ++i)
        {
            std::stringstream ss;
            ss << "test update " << i;
            record_t record;
            CHECK_TRUE(TableManager::instance().find(id, i, record, trx));
            CHECK_VALUE(record.key, i);
            std::string result(std::begin(record.value),
                               std::end(record.value));
            result =
                std::string(std::begin(result),
                            std::begin(result) + std::strlen(result.c_str()));
            CHECK_VALUE(ss.str(), result);
        }

        for (int i = 0; i < num_records; ++i)
        {
            valType v;
            std::stringstream ss;
            ss << "test update2 " << i;
            TableManager::char_to_valType(v, ss.str().c_str());
            CHECK_TRUE(TableManager::instance().update(id, i, v, trx));
        }

        for (int i = 0; i < num_records; ++i)
        {
            std::stringstream ss;
            ss << "test update2 " << i;
            record_t record;
            CHECK_TRUE(TableManager::instance().find(id, i, record, trx));
            CHECK_VALUE(record.key, i);
            std::string result(std::begin(record.value),
                               std::end(record.value));
            result =
                std::string(std::begin(result),
                            std::begin(result) + std::strlen(result.c_str()));
            CHECK_VALUE(ss.str(), result);
        }

        CHECK_TRUE(TransactionManager::instance().abort(trx));

        for (int i = 0; i < num_records; ++i)
        {
            std::stringstream ss;
            ss << "test insert " << i;
            record_t record;
            CHECK_TRUE(TableManager::instance().find(id, i, record, trx));
            CHECK_VALUE(record.key, i);
            std::string result(std::begin(record.value),
                               std::end(record.value));
            result =
                std::string(std::begin(result),
                            std::begin(result) + std::strlen(result.c_str()));
            CHECK_VALUE(ss.str(), result);
        }
    }
    END()
}

void TEST_TRANSACTION()
{
    // TEST("S1 upgrade test")
    // {
    //     LockManager::instance().reset();
    //     TransactionManager::instance().reset();

    //     int trans_id1 = TransactionManager::instance().begin();
    //     CHECK_TRUE(TransactionManager::instance().lock_acquire(1, 2,
    //     trans_id1, LockMode::SHARED));

    //     auto& table = LockManager::instance().get_table();
    //     const auto& target = table.at(LockHash(1, 2));

    //     CHECK_VALUE(target.acquire_count, 1);

    //     CHECK_TRUE(TransactionManager::instance().lock_acquire(1, 2,
    //     trans_id1, LockMode::EXCLUSIVE));

    //     CHECK_VALUE(target.acquire_count, 2);

    //     CHECK_TRUE(TransactionManager::instance().commit(trans_id1));

    //     CHECK_TRUE(table.find(LockHash(1, 2)) == table.end());
    // }
    // END()

    // TEST("S1 S2 E1 upgrade test")
    // {
    //     LockManager::instance().reset();
    //     TransactionManager::instance().reset();

    //     int trans_id1 = TransactionManager::instance().begin();
    //     int trans_id2 = TransactionManager::instance().begin();

    //     TransactionManager::instance().lock_acquire(1, 2, trans_id1,
    //     LockMode::SHARED); TransactionManager::instance().lock_acquire(1, 2,
    //     trans_id2, LockMode::SHARED);

    //     auto& table = LockManager::instance().get_table();
    //     const auto& target = table.at(LockHash(1, 2));

    //     auto p3 = std::thread([&](){
    //         TransactionManager::instance().lock_acquire(1, 2, trans_id1,
    //         LockMode::EXCLUSIVE);
    //     });
    //     p3.detach();

    //     std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    //     CHECK_TRUE(TransactionManager::instance().commit(trans_id2));
    //     CHECK_TRUE(target.wait_count == 0);
    //     CHECK_TRUE(target.acquire_count == 2);

    //     std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    //     CHECK_TRUE(TransactionManager::instance().commit(trans_id1));

    //     CHECK_TRUE(table.find(LockHash(1, 2)) == table.end());
    // }
    // END()

    // TEST("shared 3 transactions")
    // {
    //     LockManager::instance().reset();
    //     TransactionManager::instance().reset();

    //     int trans_id1 = TransactionManager::instance().begin();
    //     int trans_id2 = TransactionManager::instance().begin();
    //     int trans_id3 = TransactionManager::instance().begin();

    //     CHECK_TRUE(TransactionManager::instance().lock_acquire(1, 2,
    //     trans_id1, LockMode::SHARED));
    //     CHECK_TRUE(TransactionManager::instance().lock_acquire(1, 2,
    //     trans_id1, LockMode::SHARED));
    //     CHECK_TRUE(TransactionManager::instance().lock_acquire(1, 2,
    //     trans_id1, LockMode::SHARED));

    //     std::promise<bool> ret1, ret2;
    //     auto lock2_future = ret1.get_future();
    //     auto lock3_future = ret2.get_future();

    //     auto p1 = std::thread([&](){
    //         bool result = true;
    //         result &= TransactionManager::instance().lock_acquire(1, 2,
    //         trans_id2, LockMode::SHARED); result &=
    //         TransactionManager::instance().lock_acquire(1, 2, trans_id2,
    //         LockMode::SHARED); result &=
    //         TransactionManager::instance().lock_acquire(1, 2, trans_id2,
    //         LockMode::SHARED); ret1.set_value(result);
    //     });

    //     auto p2 = std::thread([&](){
    //         bool result = true;
    //         result &= TransactionManager::instance().lock_acquire(1, 2,
    //         trans_id3, LockMode::SHARED); result &=
    //         TransactionManager::instance().lock_acquire(1, 2, trans_id3,
    //         LockMode::SHARED); result &=
    //         TransactionManager::instance().lock_acquire(1, 2, trans_id3,
    //         LockMode::SHARED); ret2.set_value(result);
    //     });

    //     auto& table = LockManager::instance().get_table();
    //     const auto& target = table.at(LockHash(1, 2));

    //     p1.detach();
    //     p2.detach();

    //     while (target.acquire_count != 3)
    //     {
    //     }

    //     CHECK_VALUE(target.acquire_count, 3);

    //     CHECK_TRUE(TransactionManager::instance().commit(trans_id1));
    //     CHECK_VALUE(target.acquire_count, 2);

    //     CHECK_TRUE(lock2_future.get())

    //     CHECK_TRUE(TransactionManager::instance().commit(trans_id2));
    //     CHECK_VALUE(target.acquire_count, 1);

    //     CHECK_TRUE(lock3_future.get())
    // }
    // END()
}

// void TEST_LOCK()
// {
//     TEST("shared test")
//     {
//         LockManager::instance().reset();
//         TransactionManager::instance().reset();
//         int trans_id = TransactionManager::instance().begin();
//         Transaction& trans = TransactionManager::instance().get(trans_id);
//         auto lock = LockManager::instance().lock_acquire(1, 2, trans_id,
//         LockMode::SHARED);

//         CHECK_TRUE(trans.state == TransactionState::RUNNING);
//         CHECK_TRUE(trans.locks.empty());

//         CHECK_TRUE(lock->lockMode == LockMode::SHARED);
//         CHECK_FALSE(lock->wait());

//         auto& table = LockManager::instance().get_table();
//         const auto& target = table.at(LockHash(1, 2));
//         CHECK_TRUE(target.mode == LockMode::SHARED);
//         CHECK_TRUE(target.locks.front() == lock);
//         CHECK_VALUE(target.acquire_count, 1);
//         CHECK_VALUE(target.wait_count, 0);
//     }
//     END()

//     TEST("exclusive test")
//     {
//         LockManager::instance().reset();
//         TransactionManager::instance().reset();
//         int trans_id = TransactionManager::instance().begin();
//         Transaction& trans = TransactionManager::instance().get(trans_id);
//         auto lock = LockManager::instance().lock_acquire(1, 2, trans_id,
//         LockMode::EXCLUSIVE);

//         CHECK_TRUE(trans.state == TransactionState::RUNNING);
//         CHECK_TRUE(trans.locks.empty());

//         CHECK_TRUE(lock->lockMode == LockMode::EXCLUSIVE);
//         CHECK_FALSE(lock->wait());

//         auto& table = LockManager::instance().get_table();
//         const auto& target = table.at(LockHash(1, 2));
//         CHECK_TRUE(target.mode == LockMode::EXCLUSIVE);
//         CHECK_TRUE(target.locks.front() == lock);
//         CHECK_VALUE(target.acquire_count, 1);
//         CHECK_VALUE(target.wait_count, 0)
//     }
//     END()

//     TEST("shared shared test")
//     {
//         LockManager::instance().reset();
//         TransactionManager::instance().reset();
//         int trans_id1 = TransactionManager::instance().begin();
//         int trans_id2 = TransactionManager::instance().begin();
//         Transaction& trans1 = TransactionManager::instance().get(trans_id1);
//         Transaction& trans2 = TransactionManager::instance().get(trans_id2);

//         auto lock1 = LockManager::instance().lock_acquire(1, 2, trans_id1,
//         LockMode::SHARED); auto lock2 =
//         LockManager::instance().lock_acquire(1, 2, trans_id2,
//         LockMode::SHARED);

//         CHECK_TRUE(trans1.state == TransactionState::RUNNING);
//         CHECK_TRUE(trans1.locks.empty());
//         CHECK_TRUE(trans2.state == TransactionState::RUNNING);
//         CHECK_TRUE(trans2.locks.empty());

//         CHECK_TRUE(lock1->lockMode == LockMode::SHARED);
//         CHECK_FALSE(lock1->wait());
//         CHECK_TRUE(lock2->lockMode == LockMode::SHARED);
//         CHECK_FALSE(lock2->wait());

//         auto& table = LockManager::instance().get_table();
//         const auto& target = table.at(LockHash(1, 2));
//         CHECK_TRUE(target.mode == LockMode::SHARED);
//         CHECK_TRUE(target.locks.front() == lock2);
//         CHECK_TRUE(target.locks.back() == lock1);
//         CHECK_VALUE(target.acquire_count, 2);
//         CHECK_VALUE(target.wait_count, 0);
//     }
//     END()

//     TEST("release shared shared")
//     {
//         LockManager::instance().reset();
//         TransactionManager::instance().reset();
//         int trans_id1 = TransactionManager::instance().begin();
//         int trans_id2 = TransactionManager::instance().begin();

//         auto lock1 = LockManager::instance().lock_acquire(1, 2, trans_id1,
//         LockMode::SHARED); auto lock2 =
//         LockManager::instance().lock_acquire(1, 2, trans_id2,
//         LockMode::SHARED);

//         auto& table = LockManager::instance().get_table();
//         const auto& target = table.at(LockHash(1, 2));

//         CHECK_TRUE(LockManager::instance().lock_release(lock1));

//         CHECK_TRUE(target.mode == LockMode::SHARED);
//         CHECK_TRUE(target.locks.front() == lock2);
//         CHECK_VALUE(target.acquire_count, 1);
//         CHECK_VALUE(target.wait_count, 0);

//         CHECK_TRUE(LockManager::instance().lock_release(lock2));
//         CHECK_TRUE(table.find(LockHash(1, 2)) == table.end());
//     }
//     END()

//     TEST("release exclusive")
//     {
//         LockManager::instance().reset();
//         TransactionManager::instance().reset();
//         int trans_id = TransactionManager::instance().begin();
//         auto lock = LockManager::instance().lock_acquire(1, 2, trans_id,
//         LockMode::EXCLUSIVE);

//         auto& table = LockManager::instance().get_table();

//         CHECK_TRUE(LockManager::instance().lock_release(lock));

//         CHECK_TRUE(table.find(LockHash(1, 2)) == table.end());
//     }
//     END()

//     TEST("S1 E2 S3 test")
//     {
//         LockManager::instance().reset();
//         TransactionManager::instance().reset();

//         int trans_id1 = TransactionManager::instance().begin();
//         int trans_id2 = TransactionManager::instance().begin();
//         int trans_id3 = TransactionManager::instance().begin();

//         auto lock1 = LockManager::instance().lock_acquire(1, 2, trans_id1,
//         LockMode::SHARED);

//         std::promise<decltype(lock1)> ret1, ret2;
//         auto lock2_future = ret1.get_future();
//         auto lock3_future = ret2.get_future();

//         auto& table = LockManager::instance().get_table();
//         const auto& target = table.at(LockHash(1, 2));

//         CHECK_TRUE(target.mode == LockMode::SHARED);
//         CHECK_TRUE(target.locks.front() == lock1);

//         auto p1 = std::thread([&](){
//             ret1.set_value(LockManager::instance().lock_acquire(1, 2,
//             trans_id2, LockMode::EXCLUSIVE));
//         });
//         p1.detach();

//         while (target.wait_count != 1)
//         {}

//         CHECK_VALUE(target.acquire_count, 1);
//         CHECK_VALUE(target.wait_count, 1);
//         CHECK_TRUE(target.mode == LockMode::EXCLUSIVE);
//         CHECK_TRUE(target.locks.front() == lock1);

//         auto p2 = std::thread([&](){
//             ret2.set_value(LockManager::instance().lock_acquire(1, 2,
//             trans_id3, LockMode::SHARED));
//         });

//         p2.detach();

//         while (target.wait_count != 2)
//         {}

//         CHECK_VALUE(target.acquire_count, 1);
//         CHECK_VALUE(target.wait_count, 2);
//         CHECK_TRUE(target.mode == LockMode::EXCLUSIVE);
//         CHECK_TRUE(target.locks.front() == lock1);

//         CHECK_TRUE(LockManager::instance().lock_release(lock1));

//         auto lock2 = lock2_future.get();

//         CHECK_TRUE(target.mode == LockMode::EXCLUSIVE);
//         CHECK_TRUE(target.locks.front() == lock2);
//         CHECK_VALUE(target.acquire_count, 1);
//         CHECK_VALUE(target.wait_count, 1);

//         CHECK_TRUE(LockManager::instance().lock_release(lock2));

//         auto lock3 = lock3_future.get();

//         CHECK_TRUE(target.mode == LockMode::SHARED);
//         CHECK_TRUE(target.locks.front() == lock3);
//         CHECK_VALUE(target.acquire_count, 1);
//         CHECK_VALUE(target.wait_count, 0);
//     }
//     END()

//     TEST("one lock deadlock test - S1 E2 E1")
//     {
//         LockManager::instance().reset();
//         TransactionManager::instance().reset();

//         int trans_id1 = TransactionManager::instance().begin();
//         int trans_id2 = TransactionManager::instance().begin();

//         auto lock1 = LockManager::instance().lock_acquire(1, 2, trans_id1,
//         LockMode::SHARED);

//         std::promise<decltype(lock1)> ret1;
//         auto lock2_future = ret1.get_future();

//         auto p1 = std::thread([&](){
//             ret1.set_value(LockManager::instance().lock_acquire(1, 2,
//             trans_id2, LockMode::EXCLUSIVE));
//         });

//         auto& table = LockManager::instance().get_table();
//         const auto& target = table.at(LockHash(1, 2));

//         p1.detach();

//         while (target.wait_count != 1)
//         {}

//         CHECK_TRUE(TransactionManager::instance().get_transactions().size()
//         == 2);

//         auto invalid = LockManager::instance().lock_upgrade(1, 2, trans_id1,
//         LockMode::EXCLUSIVE);

//         CHECK_TRUE(TransactionManager::instance().get_transactions().size()
//         == 1);

//         CHECK_TRUE(invalid == nullptr);
//     }
//     END()
// }

// void TEST_MULTITHREADING()
// {
//     auto process = [](int repeat, int num_records, int process_id,
//                       int table_id) {
//         int id = process_id;
//         for (int j = 0; j < repeat; ++j)
//         {
//             TEST("insert num_records to table")
//             {
//                 for (int i = 0; i < num_records; ++i)
//                 {
//                     valType v;
//                     std::stringstream ss;
//                     ss << "test insert " << i;
//                     TableManager::char_to_valType(v, ss.str().c_str());
//                     CHECK_TRUE(TableManager::instance().insert(id, i, v));
//                 }
//             }
//             END()

//             TEST("find num_records to table")
//             {
//                 for (int i = 0; i < num_records; ++i)
//                 {
//                     std::stringstream ss;
//                     ss << "test insert " << i;
//                     record_t record;
//                     CHECK_TRUE(TableManager::instance().find(id, i, record));
//                     CHECK_VALUE(record.key, i);
//                     std::string result(std::begin(record.value),
//                                        std::end(record.value));
//                     result = std::string(
//                         std::begin(result),
//                         std::begin(result) + std::strlen(result.c_str()));
//                     CHECK_VALUE(ss.str(), result);
//                 }
//             }
//             END()

//             TEST("update num_records to table")
//             {
//                 for (int i = 0; i < num_records; ++i)
//                 {
//                     valType v;
//                     std::stringstream ss;
//                     ss << "test update " << i;
//                     TableManager::char_to_valType(v, ss.str().c_str());
//                     CHECK_TRUE(TableManager::instance().update(id, i, v));
//                 }
//             }
//             END()

//             TEST("find after update num_records to table")
//             {
//                 for (int i = 0; i < num_records; ++i)
//                 {
//                     std::stringstream ss;
//                     ss << "test update " << i;
//                     record_t record;
//                     CHECK_TRUE(TableManager::instance().find(id, i, record));
//                     CHECK_VALUE(record.key, i);
//                     std::string result(std::begin(record.value),
//                                        std::end(record.value));
//                     result = std::string(
//                         std::begin(result),
//                         std::begin(result) + std::strlen(result.c_str()));
//                     CHECK_VALUE(ss.str(), result);
//                 }
//             }
//             END()

//             TEST("delete num_records to table")
//             {
//                 for (int i = 0; i < num_records; ++i)
//                 {
//                     record_t record;
//                     CHECK_TRUE(TableManager::instance().delete_key(id, i));
//                     CHECK_FALSE(TableManager::instance().find(id, i,
//                     record));
//                 }
//             }
//             END()
//         }

//         std::cout << "\n";
//     };

//     auto buffer_size = 10000;
//     auto num_records = 10000;
//     auto repeat = 3;
//     auto test_index = 0;
//     auto thread_num = 10;

//     // int repeat, int num_records, int process_id, int table_id

//     auto test_multithreading = [&]() {
//         TEST("init db")
//         {
//             CHECK_TRUE(TableManager::instance().init_db(buffer_size));
//         }
//         END()
//         std::vector<int> ids;
//         TEST("open tables")
//         {
//             for (int i = 0; i < thread_num; ++i)
//             {
//                 std::stringstream ss;
//                 ss << "multi_" << test_index << "_" << i << ".db";
//                 int id;
//                 ids.push_back(id =
//                 TableManager::instance().open_table(ss.str())); CHECK_TRUE(id
//                 != INVALID_TABLE_ID);
//             }
//         }
//         END()

//         std::vector<std::thread> threads;
//         for (int i = 0; i < thread_num; ++i)
//         {
//             threads.emplace_back([&process, &ids, repeat, num_records, i,
//             test_index]() {
//                 process(repeat, num_records, ids[i], test_index);
//             });
//         }

//         for (auto& t : threads)
//         {
//             t.join();
//         }

//         TEST("shutdown db")
//         {
//             CHECK_TRUE(TableManager::instance().shutdown_db());
//         }
//         END()

//         TEST("use table after shutdown")
//         {
//             record_t record;
//             valType val;
//             CHECK_FALSE(TableManager::instance().close_table(1));
//             CHECK_FALSE(TableManager::instance().delete_key(1, 1));
//             CHECK_FALSE(TableManager::instance().find(1, 1, record));
//             CHECK_FALSE(TableManager::instance().insert(1, 1, val));
//             CHECK_VALUE(TableManager::instance().open_table("shutdown.db"),
//                         INVALID_TABLE_ID);
//         }
//         END()
//     };

//     test_multithreading();
// }

void TEST_TABLE()
{
    // auto buffer_size = 10000;
    // auto num_records = 100000;
    // auto repeat = 3;
    // auto table_num = 1;
    // auto test_index = 0;
    // auto test_table = [&]() {
    //     TEST("init db")
    //     {
    //         CHECK_TRUE(TableManager::instance().init_db(buffer_size));
    //     }
    //     END()
    //     for (int j = 0; j < repeat; ++j)
    //     {
    //         std::cout << "  repeat table test " << j << '\n';
    //         std::vector<int> tables;
    //         TEST("open tables")
    //         {
    //             for (int i = 0; i < table_num; ++i)
    //             {
    //                 std::stringstream ss;
    //                 ss << "table_" << test_index << "_" << i << ".db";

    //                 int id = TableManager::instance().open_table(ss.str());
    //                 CHECK_TRUE(id != INVALID_TABLE_ID);
    //                 tables.push_back(id);
    //             }
    //         }
    //         END()

    //         TEST("insert num_records to 10 tables")
    //         {
    //             for (int i = 0; i < num_records; ++i)
    //             {
    //                 valType v;
    //                 std::stringstream ss;
    //                 ss << "test insert " << i;
    //                 TableManager::char_to_valType(v, ss.str().c_str());
    //                 int mul = 0;
    //                 for (auto id : tables)
    //                 {
    //                     CHECK_TRUE(TableManager::instance().insert(
    //                         id, i + mul * num_records, v));
    //                     ++mul;
    //                 }
    //             }
    //         }
    //         END()

    //         TEST("find num_records to 10 tables")
    //         {
    //             for (int i = 0; i < num_records; ++i)
    //             {
    //                 std::stringstream ss;
    //                 ss << "test insert " << i;
    //                 int mul = 0;
    //                 for (auto id : tables)
    //                 {
    //                     record_t record;
    //                     CHECK_TRUE(TableManager::instance().find(
    //                         id, i + mul * num_records, record));
    //                     CHECK_VALUE(record.key, i + mul * num_records);
    //                     std::string result(std::begin(record.value),
    //                                        std::end(record.value));
    //                     result = std::string(
    //                         std::begin(result),
    //                         std::begin(result) +
    //                         std::strlen(result.c_str()));
    //                     CHECK_VALUE(ss.str(), result);
    //                     ++mul;
    //                 }
    //             }
    //         }
    //         END()

    //         TEST("update num_records to 10 tables")
    //         {
    //             for (int i = 0; i < num_records; ++i)
    //             {
    //                 valType v;
    //                 std::stringstream ss;
    //                 ss << "test update " << i;
    //                 TableManager::char_to_valType(v, ss.str().c_str());
    //                 int mul = 0;
    //                 for (auto id : tables)
    //                 {
    //                     CHECK_TRUE(TableManager::instance().update(
    //                         id, i + mul * num_records, v));
    //                     ++mul;
    //                 }
    //             }
    //         }
    //         END()

    //         TEST("find after update num_records to 10 tables")
    //         {
    //             for (int i = 0; i < num_records; ++i)
    //             {
    //                 std::stringstream ss;
    //                 ss << "test update " << i;
    //                 int mul = 0;
    //                 for (auto id : tables)
    //                 {
    //                     record_t record;
    //                     CHECK_TRUE(TableManager::instance().find(
    //                         id, i + mul * num_records, record));
    //                     CHECK_VALUE(record.key, i + mul * num_records);
    //                     std::string result(std::begin(record.value),
    //                                        std::end(record.value));
    //                     result = std::string(
    //                         std::begin(result),
    //                         std::begin(result) +
    //                         std::strlen(result.c_str()));
    //                     CHECK_VALUE(ss.str(), result);
    //                     ++mul;
    //                 }
    //             }
    //         }
    //         END()

    //         TEST("delete num_records to 10 tables")
    //         {
    //             for (int i = 0; i < num_records; ++i)
    //             {
    //                 int mul = 0;
    //                 for (auto id : tables)
    //                 {
    //                     record_t record;
    //                     CHECK_TRUE(TableManager::instance().delete_key(
    //                         id, i + mul * num_records));
    //                     CHECK_FALSE(TableManager::instance().find(
    //                         id, i + mul * num_records, record));
    //                     ++mul;
    //                 }
    //             }
    //         }
    //         END()

    //         if (table_num == 10)
    //         {
    //             TEST("open 11th table failure before close tables")
    //             {
    //                 int id =
    //                 TableManager::instance().open_table("invalid.db");
    //                 CHECK_VALUE(id, INVALID_TABLE_ID);
    //             }
    //             END()
    //         }

    //         TEST("close tables")
    //         {
    //             for (auto id : tables)
    //             {
    //                 CHECK_TRUE(TableManager::instance().close_table(id));
    //             }
    //         }
    //         END()

    //         if (table_num == 10)
    //         {
    //             TEST("open 11th table failure after close tables")
    //             {
    //                 int id =
    //                 TableManager::instance().open_table("invalid.db");
    //                 CHECK_VALUE(id, INVALID_TABLE_ID);
    //             }
    //             END()
    //         }
    //         std::cout << "\n";
    //     }

    //     TEST("shutdown db")
    //     {
    //         CHECK_TRUE(TableManager::instance().shutdown_db());
    //     }
    //     END()

    //     TEST("use table after shutdown")
    //     {
    //         record_t record;
    //         valType val;
    //         CHECK_FALSE(TableManager::instance().close_table(1));
    //         CHECK_FALSE(TableManager::instance().delete_key(1, 1));
    //         CHECK_FALSE(TableManager::instance().find(1, 1, record));
    //         CHECK_FALSE(TableManager::instance().insert(1, 1, val));
    //         CHECK_VALUE(TableManager::instance().open_table("shutdown.db"),
    //                     INVALID_TABLE_ID);
    //     }
    //     END()
    // };

    // constexpr auto small_buffer = 10;
    // constexpr auto middle_buffer = 10000;
    // constexpr auto big_buffer = 1000000;

    // ++test_index;
    // buffer_size = middle_buffer;
    // std::cout << "\nopen 10 table with same buffer\n";
    // test_table();

    // ++test_index;
    // buffer_size = small_buffer;
    // std::cout << "open 10 table with small buffer\n";
    // test_table();

    // ++test_index;
    // buffer_size = big_buffer;
    // std::cout << "open 10 table with big buffer";
    // test_table();

    // ++test_index;
    // buffer_size = middle_buffer;
    // repeat = 1;
    // table_num = 1;
    // num_records = 1000000;
    // std::cout << "many records test: " << num_records << '\n';
    // test_table();
}

void TEST_POD()
{
    using array = std::array<uint8_t, 1>;
    TEST("is_standard_layout std::array")
    {
        CHECK_TRUE(std::is_standard_layout<array>::value);
    }
    END()
    TEST("is_standard_layout HeaderPageHeader")
    {
        CHECK_TRUE(std::is_standard_layout<HeaderPageHeader>::value);
    }
    END()
    TEST("is_standard_layout NodePageHeader")
    {
        CHECK_TRUE(std::is_standard_layout<NodePageHeader>::value);
    }
    END()
    TEST("is_standard_layout FreePageHeader")
    {
        CHECK_TRUE(std::is_standard_layout<FreePageHeader>::value);
    }
    END()
    TEST("is_standard_layout page_t")
    {
        CHECK_TRUE(std::is_standard_layout<page_t>::value);
    }
    END()
    TEST("is_standard_layout Record")
    {
        CHECK_TRUE(std::is_standard_layout<Record>::value);
    }
    END()
    TEST("is_standard_layout Internal")
    {
        CHECK_TRUE(std::is_standard_layout<Internal>::value);
    }
    END()
    TEST("is_standard_layout Records")
    {
        CHECK_TRUE(std::is_standard_layout<Records>::value);
    }
    END()
    TEST("is_standard_layout Internals")
    {
        CHECK_TRUE(std::is_standard_layout<Internals>::value);
    }
    END()

    TEST("is_trivial std::array")
    {
        CHECK_TRUE(std::is_standard_layout<array>::value);
    }
    END()
    TEST("is_trivial HeaderPageHeader")
    {
        CHECK_TRUE(std::is_trivial<HeaderPageHeader>::value);
    }
    END()
    TEST("is_trivial NodePageHeader")
    {
        CHECK_TRUE(std::is_trivial<NodePageHeader>::value);
    }
    END()
    TEST("is_trivial FreePageHeader")
    {
        CHECK_TRUE(std::is_trivial<FreePageHeader>::value);
    }
    END()
    TEST("is_trivial page_t")
    {
        CHECK_TRUE(std::is_trivial<page_t>::value);
    }
    END()
    TEST("is_trivial Record")
    {
        CHECK_TRUE(std::is_trivial<Record>::value);
    }
    END()
    TEST("is_trivial Internal")
    {
        CHECK_TRUE(std::is_trivial<Internal>::value);
    }
    END()
    TEST("is_trivial Records")
    {
        CHECK_TRUE(std::is_trivial<Records>::value);
    }
    END()
    TEST("is_trivial Internals")
    {
        CHECK_TRUE(std::is_trivial<Internals>::value);
    }
    END()
}

void TEST_FILE_MANAGER()
{
    // TEST("FileManager Page r/w")
    // {
    //     FileManager fm;
    //     fm.open("test.db");

    //     page_t page {};
    //     for (int i = 0; i < 248; ++i)
    //     {
    //         page.internals()[i].key = i;
    //         page.internals()[i].node_id = i;
    //     }

    //     fm.commit(0, page);

    //     page_t readPage {};
    //     fm.load(0, readPage);

    //     for (int i = 0; i < 248; ++i)
    //     {
    //         CHECK_TRUE(page.internals()[i].key ==
    //         readPage.internals()[i].key);
    //         CHECK_TRUE(page.internals()[i].node_id ==
    //                    readPage.internals()[i].node_id);
    //     }
    // }
    // END()

    // TEST("FileManager Many Page")
    // {
    //     FileManager fm;
    //     fm.open("qqq.db");
    //     page_t page {};
    //     for (int i = 0; i < 248; ++i)
    //     {
    //         page.internals()[i].key = i;
    //         page.internals()[i].node_id = i;
    //     }
    //     std::vector<pagenum_t> nums;
    //     for (int i = 0; i < 100; ++i)
    //     {
    //         pagenum_t num = fm.create();
    //         CHECK_TRUE(num != EMPTY_PAGE_NUMBER);

    //         CHECK_TRUE(fm.commit(num, page));
    //         nums.push_back(num);
    //     }

    //     for (auto n : nums)
    //     {
    //         CHECK_TRUE(fm.load(n, page));
    //     }
    // }
    // END()
}

void TEST_BPT()
{
    // constexpr auto count = 1000000;
    // TableManager::instance().init_db(1000);
    // TEST("BPTree random number insert many")
    // {
    //     BPTree tree;
    //     CHECK_TRUE(tree.open_table("insert.db"));

    //     std::vector<int> keys(count);

    //     std::random_device rd;
    //     std::mt19937 mt(rd());

    //     for (int i = 1; i <= count; ++i)
    //     {
    //         std::uniform_int_distribution<int> range(0, i - 1);
    //         int pos = range(mt);
    //         keys[i - 1] = keys[pos];
    //         keys[pos] = i;
    //     }

    //     for (int a : keys)
    //     {
    //         valType v;
    //         std::stringstream ss;
    //         ss << "test insert " << a;
    //         tree.char_to_valType(v, ss.str().c_str());

    //         CHECK_TRUE(tree.insert(a, v));
    //     }
    // }
    // END()

    // TEST("BPTree find key")
    // {
    //     BPTree tree;
    //     CHECK_TRUE(tree.open_table("insert.db"));

    //     record_t record;

    //     for (int i = 1; i <= count; ++i)
    //     {
    //         CHECK_TRUE(tree.find(i, record));
    //         CHECK_VALUE(record.key, i);
    //     }
    // }
    // END()

    // TEST("BPTree delete half - 1")
    // {
    //     BPTree tree;
    //     CHECK_TRUE(tree.open_table("insert.db"));

    //     record_t record;

    //     for (int i = 1; i <= count / 2; ++i)
    //     {
    //         CHECK_TRUE(tree.delete_key(i));
    //         CHECK_FALSE(tree.find(i, record));
    //     }

    //     for (int i = count / 2 + 1; i <= count; ++i)
    //     {
    //         CHECK_TRUE(tree.find(i, record));
    //         CHECK_VALUE(record.key, i);
    //     }
    // }
    // END()

    // TEST("BPTree delete half - 2")
    // {
    //     BPTree tree;
    //     CHECK_TRUE(tree.open_table("insert.db"));

    //     record_t record;

    //     for (int i = count / 2 + 1; i <= count; ++i)
    //     {
    //         CHECK_TRUE(tree.delete_key(i));
    //         CHECK_FALSE(tree.find(i, record));
    //     }
    // }
    // END()

    // TEST("BPTree insert after delete")
    // {
    //     BPTree tree;
    //     CHECK_TRUE(tree.open_table("insert.db"));

    //     for (int i = count; i > 0; --i)
    //     {
    //         valType v;
    //         std::stringstream ss;
    //         ss << "test insert " << i;
    //         tree.char_to_valType(v, ss.str().c_str());

    //         CHECK_TRUE(tree.insert(i, v));
    //     }
    // }
    // END()

    // TEST("BPTree find after delete and insert")
    // {
    //     BPTree tree;
    //     CHECK_TRUE(tree.open_table("insert.db"));

    //     record_t record;

    //     for (int i = 1; i <= count; ++i)
    //     {
    //         CHECK_TRUE(tree.find(i, record));
    //         CHECK_VALUE(record.key, i);
    //     }
    // }
    // END()

    // TEST("BPTree delete all")
    // {
    //     BPTree tree;
    //     CHECK_TRUE(tree.open_table("insert.db"));

    //     record_t record;

    //     for (int i = count; i >= 1; --i)
    //     {
    //         CHECK_TRUE(tree.delete_key(i));
    //         CHECK_FALSE(tree.find(i, record));
    //     }
    // }
    // END()
}

void TEST_FILE()
{
}

void TESTS()
{
    TEST("CHECK_TRUE")
    {
        CHECK_TRUE(1 == 1);
        CHECK_TRUE(true);
        CHECK_TRUE(2);
        CHECK_TRUE(-1);
    }
    END()

    TEST("CHECK_FALSE")
    {
        CHECK_FALSE(1 == 2);
        CHECK_FALSE(0);
        CHECK_FALSE(false);
    }
    END()

    TEST("CHECK_VALUE")
    {
        CHECK_VALUE(1, 1);
        CHECK_VALUE(1 + 1, 2);
    }
    END()

    TEST("CHECK_NULLPTR")
    {
        CHECK_NULLPTR(nullptr);
    }
    END()
}
