#include "test.hpp"

#include <cstdio>

#include <iostream>
#include <list>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "bptree.hpp"
#include "file_api.hpp"
#include "file_manager.hpp"
#include "logger.hpp"

int success, cnt, allSuccess, allCnt;
bool testFlag;
std::string testName;

void TEST_FILE_MANAGER();
void TEST_BPT();
void TEST_FILE();
void TESTS();

int main()
{
    void (*tests[])() = { TEST_FILE_MANAGER, TEST_BPT, TEST_FILE, TESTS };

    std::string testNames[] = { "file_manager", "bpt", "test_file", "TESTS" };

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

void TEST_FILE_MANAGER()
{
    TEST("FileManager")
    {
        FileManager fm;

        const HeaderPageHeader& ph = fm.fileHeader;

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

        const HeaderPageHeader& ph = fm.fileHeader;

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

        page_t page;
        for (int i = 0; i < 248; ++i)
        {
            page.internals()[i].key = i;
            page.internals()[i].node_id = i;
        }

        fm.pageWrite(0, page);

        page_t readPage;
        fm.pageRead(0, readPage);

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
        page_t page;
        for (int i = 0; i < 248; ++i)
        {
            page.internals()[i].key = i;
            page.internals()[i].node_id = i;
        }
        std::vector<pagenum_t> nums;
        for (int i = 0; i < 100; ++i)
        {
            pagenum_t num = fm.pageCreate();
            CHECK_TRUE(num != EMPTY_PAGE_NUMBER);

            CHECK_TRUE(fm.pageWrite(num, page));
            nums.push_back(num);
        }

        for (auto n : nums)
        {
            CHECK_TRUE(fm.pageRead(n, page));
        }
    }
    END()

    TEST("FileManager free many")
    {
        FileManager fm;
        fm.open("qqq.db");
        for (int i = 1; i <= 100; ++i)
        {
            fm.pageFree(i);
        }
        pagenum_t now = fm.fileHeader.freePageNumber;
        CHECK_VALUE(now, 100);
        while (now != EMPTY_PAGE_NUMBER)
        {
            page_t page;
            CHECK_TRUE(fm.pageRead(now, page));
            auto& header = page.freePageHeader();
            CHECK_VALUE(header.nextFreePageNumber + 1, now);
            now = header.nextFreePageNumber;
        }
    }
    END()
}

void TEST_BPT()
{
    TEST("BPTree random number insert many")
    {
        BPTree tree;
        CHECK_TRUE(tree.open_table("insert.db"));

        std::vector<int> keys(100000);

        std::random_device rd;
        std::mt19937 mt(rd());

        for (int i = 1; i <= 100000; ++i)
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

    //  TEST("BPTree  insert many")
    // {
    //     BPTree tree;
    //     CHECK_TRUE(tree.open_table("insert.db"));

    //     for (int i = 100000; i > 0; --i)
    // {
    //         valType v;
    //         std::stringstream ss;
    //         ss << "test insert " << i;
    //         tree.char_to_valType(v, ss.str().c_str());

    //         CHECK_TRUE(tree.insert(i, v));
    //     }
    // }
    // END()

    TEST("BPTree find key")
    {
        BPTree tree;
        CHECK_TRUE(tree.open_table("insert.db"));

        for (int i = 1; i <= 100000; ++i)
        {
            auto record = tree.find(i);
            if (!record)
            {
                std::cout << i << '\n';
            }
            CHECK_TRUE(record);
        }
    }
    END()

    TEST("BPTree delete half - 1")
    {
        BPTree tree;
        CHECK_TRUE(tree.open_table("insert.db"));

        for (int i = 1; i <= 50000; ++i)
        {
            CHECK_TRUE(tree.delete_key(i));
            CHECK_FALSE(tree.find(i));
        }

        for (int i = 50001; i <= 100000; ++i)
        {
            CHECK_TRUE(tree.find(i));
        }
    }
    END()

    TEST("BPTree delete half - 2")
    {
        BPTree tree;
        CHECK_TRUE(tree.open_table("insert.db"));

        for (int i = 50001; i <= 100000; ++i)
        {
            // std::cout << i << '\n';
            CHECK_TRUE(tree.delete_key(i));
            CHECK_FALSE(tree.find(i));
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
