#include "test.hpp"

#include <cstdio>

#include <iostream>
#include <string>
#include <sstream>
#include <vector>

#include "file_api.hpp"
#include "file_manager.hpp"
#include "bpt.hpp"
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

    for (int i = 0; i < (sizeof(tests) / sizeof(void (*)(void))); ++i)
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
            page.entry.internals[i].key = i;
            page.entry.internals[i].pageNumber = i;
        }

        fm.pageWrite(0, page);

        page_t readPage;
        fm.pageRead(0, readPage);

        for (int i = 0; i < 248; ++i)
        {
            CHECK_TRUE(page.entry.internals[i].key ==
                       readPage.entry.internals[i].key);
            CHECK_TRUE(page.entry.internals[i].pageNumber ==
                       readPage.entry.internals[i].pageNumber);
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
            page.entry.internals[i].key = i;
            page.entry.internals[i].pageNumber = i;
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
}

void TEST_BPT()
{
    TEST("BPTree")
    {
        FileManager fm;
        fm.open("test.db");

        BPTree tree(fm);
    }
    END() 

    TEST("BPTree insert")
    {
        FileManager fm;
        fm.open("insert.db");

        BPTree tree(fm, true);

        for (int i = 100; i > 0; --i)
        {
            valType v;
            std::stringstream ss;
            ss << "test insert " << i;
            tree.char_to_valType(v, ss.str().c_str());

            CHECK_TRUE(tree.insert(i, v));
        }
    }
    END()

    TEST("BPTree insert open")
    {
        FileManager fm;
        fm.open("insert.db");

        std::cout << fm.fileHeader;

        BPTree tree(fm, true);
    }
    END()

    TEST("BPTree insert many")
    {
        FileManager fm;
        fm.open("insert.db");

        BPTree tree(fm, true);

        for (int i = 10000; i > 100; --i)
        {
            valType v;
            std::stringstream ss;
            ss << "test insert " << i;
            tree.char_to_valType(v, ss.str().c_str());

            CHECK_TRUE(tree.insert(i, v));
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
