#include "test.hpp"

#include <cstdio>
#include <iostream>
#include <list>
#include <random>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include "bptree.hpp"
#include "file_api.hpp"
#include "file_manager.hpp"
#include "logger.hpp"
#include "table_manager.hpp"

int success, cnt, allSuccess, allCnt;
bool testFlag;
std::string testName;

void TEST_FILE_MANAGER();
void TEST_POD();
void TEST_BPT();
void TEST_FILE();
void TESTS();
void TEST_TABLE();

int main()
{
    void (*tests[])() = { TEST_POD, TEST_FILE_MANAGER, TEST_TABLE,
                          TEST_BPT, TEST_FILE,         TESTS };

    std::string testNames[] = { "test pod", "file_manager", "table",
                                "bpt",      "test_file",    "TESTS" };

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

void TEST_TABLE()
{
    return;
    for (int j = 0; j < 3; ++j)
    {
        std::vector<int> tables;
        TEST("open 10 tables")
        {
            for (int i = 0; i < 10; ++i)
            {
                std::stringstream ss;
                ss << "table_" << i << ".db";

                int id = TableManager::instance().open_table(ss.str());
                CHECK_TRUE(id != INVALID_TABLE_ID);
                tables.push_back(id);
            }
        }
        END()

        TEST("insert 10000 to 10 tables")
        {
            for (int i = 0; i < 10000; ++i)
            {
                valType v;
                std::stringstream ss;
                ss << "test insert " << i;
                TableManager::char_to_valType(v, ss.str().c_str());

                int mul = 0;
                for (auto id : tables)
                {
                    CHECK_TRUE(TableManager::instance().insert(
                        id, i + mul * 10000, v));
                    ++mul;
                }
            }
        }
        END()

        TEST("find 10000 to 10 tables")
        {
            for (int i = 0; i < 10000; ++i)
            {
                int mul = 0;
                for (auto id : tables)
                {
                    record_t record;
                    CHECK_TRUE(TableManager::instance().find(
                        id, i + mul * 10000, record));
                    CHECK_VALUE(record.key, i + mul * 10000);
                    ++mul;
                }
            }
        }
        END()

        TEST("delete 10000 to 10 tables")
        {
            for (int i = 0; i < 10000; ++i)
            {
                int mul = 0;
                for (auto id : tables)
                {
                    record_t record;
                    CHECK_TRUE(TableManager::instance().delete_key(
                        id, i + mul * 10000));
                    CHECK_FALSE(TableManager::instance().find(
                        id, i + mul * 10000, record));
                    ++mul;
                }
            }
        }
        END()

        TEST("close 10 tables")
        {
            for (auto id : tables)
            {
                CHECK_TRUE(TableManager::instance().close_table(id));
            }
        }
        END()
    }
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
    TEST("FileManager")
    {
        FileManager fm;

        const HeaderPageHeader& ph = fm.getFileHeader();

        CHECK_VALUE(ph.freePageNumber, 0);
        CHECK_VALUE(ph.numberOfPages, 0);
        CHECK_VALUE(ph.rootPageNumber, 0);

        for (int i = 0; i < 104; ++i)
        {
            CHECK_VALUE(ph.reserved[i], 0);
        }
    }
    END()

    TEST("FileManager::open")
    {
        FileManager fm;
        fm.open("test.db");

        const HeaderPageHeader& ph = fm.getFileHeader();

        CHECK_VALUE(ph.freePageNumber, 0);
        CHECK_VALUE(ph.numberOfPages, 1);
        CHECK_VALUE(ph.rootPageNumber, 0);

        for (int i = 0; i < 104; ++i)
        {
            CHECK_VALUE(ph.reserved[i], 0);
        }
    }
    END()

    TEST("FileManager Page r/w")
    {
        FileManager fm;
        fm.open("test.db");

        page_t page {};
        for (int i = 0; i < 248; ++i)
        {
            page.internals()[i].key = i;
            page.internals()[i].node_id = i;
        }

        fm.commit(0, page);

        page_t readPage {};
        fm.load(0, readPage);

        for (int i = 0; i < 248; ++i)
        {
            CHECK_TRUE(page.internals()[i].key == readPage.internals()[i].key);
            CHECK_TRUE(page.internals()[i].node_id ==
                       readPage.internals()[i].node_id);
        }
    }
    END()

    TEST("FileManager Many Page")
    {
        FileManager fm;
        fm.open("qqq.db");
        page_t page {};
        for (int i = 0; i < 248; ++i)
        {
            page.internals()[i].key = i;
            page.internals()[i].node_id = i;
        }
        std::vector<pagenum_t> nums;
        for (int i = 0; i < 100; ++i)
        {
            pagenum_t num = fm.create();
            CHECK_TRUE(num != EMPTY_PAGE_NUMBER);

            CHECK_TRUE(fm.commit(num, page));
            nums.push_back(num);
        }

        for (auto n : nums)
        {
            CHECK_TRUE(fm.load(n, page));
        }
    }
    END()

    TEST("FileManager free many")
    {
        FileManager fm;
        fm.open("qqq.db");
        for (int i = 1; i <= 100; ++i)
        {
            fm.free(i);
        }
        pagenum_t now = fm.getFileHeader().freePageNumber;
        CHECK_VALUE(now, 100);
        while (now != EMPTY_PAGE_NUMBER)
        {
            page_t page {};
            CHECK_TRUE(fm.load(now, page));
            auto& header = page.freePageHeader();
            CHECK_VALUE(header.nextFreePageNumber + 1, now);
            now = header.nextFreePageNumber;
        }
    }
    END()
}

void TEST_BPT()
{
    constexpr auto count = 100000;
    TEST("BPTree random number insert many")
    {
        BPTree tree;
        CHECK_TRUE(tree.open_table("insert.db"));

        std::vector<int> keys(count);

        std::random_device rd;
        std::mt19937 mt(rd());

        for (int i = 1; i <= count; ++i)
        {
            std::uniform_int_distribution<int> range(0, i - 1);
            int pos = range(mt);
            keys[i - 1] = keys[pos];
            keys[pos] = i;
        }

        for (int a : keys)
        {
            valType v;
            std::stringstream ss;
            ss << "test insert " << a;
            tree.char_to_valType(v, ss.str().c_str());

            CHECK_TRUE(tree.insert(a, v));
        }
    }
    END()

    TEST("BPTree find key")
    {
        BPTree tree;
        CHECK_TRUE(tree.open_table("insert.db"));

        record_t record;

        for (int i = 1; i <= count; ++i)
        {
            CHECK_TRUE(tree.find(i, record));
            CHECK_VALUE(record.key, i);
        }
    }
    END()

    TEST("BPTree delete half - 1")
    {
        BPTree tree;
        CHECK_TRUE(tree.open_table("insert.db"));

        record_t record;

        for (int i = 1; i <= count / 2; ++i)
        {
            CHECK_TRUE(tree.delete_key(i));
            CHECK_FALSE(tree.find(i, record));
        }

        for (int i = count / 2 + 1; i <= count; ++i)
        {
            CHECK_TRUE(tree.find(i, record));
            CHECK_VALUE(record.key, i);
        }
    }
    END()

    TEST("BPTree delete half - 2")
    {
        BPTree tree;
        CHECK_TRUE(tree.open_table("insert.db"));

        record_t record;

        for (int i = count / 2 + 1; i <= count; ++i)
        {
            CHECK_TRUE(tree.delete_key(i));
            CHECK_FALSE(tree.find(i, record));
        }
    }
    END()

    TEST("BPTree insert after delete")
    {
        BPTree tree;
        CHECK_TRUE(tree.open_table("insert.db"));

        for (int i = count; i > 0; --i)
        {
            valType v;
            std::stringstream ss;
            ss << "test insert " << i;
            tree.char_to_valType(v, ss.str().c_str());

            CHECK_TRUE(tree.insert(i, v));
        }
    }
    END()

    TEST("BPTree find after delete and insert")
    {
        BPTree tree;
        CHECK_TRUE(tree.open_table("insert.db"));

        record_t record;

        for (int i = 1; i <= count; ++i)
        {
            CHECK_TRUE(tree.find(i, record));
            CHECK_VALUE(record.key, i);
        }
    }
    END()

    TEST("BPTree delete all")
    {
        BPTree tree;
        CHECK_TRUE(tree.open_table("insert.db"));

        record_t record;

        for (int i = count; i >= 1; --i)
        {
            CHECK_TRUE(tree.delete_key(i));
            CHECK_FALSE(tree.find(i, record));
        }
    }
    END()
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
