#include "test.hpp"

#include <stdbool.h>
#include <stdio.h>

#include <iostream>
#include <string>

#include "file.hpp"
#include "file_manager.hpp"

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

        const HeaderPageHeader& ph = fm.getHeaderPageHeader();

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

        const HeaderPageHeader& ph = fm.getHeaderPageHeader();

        CHECK_VALUE(ph.freePageNumber, 0);
        CHECK_VALUE(ph.numberOfPages, 0);
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
}

void TEST_BPT()
{
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
