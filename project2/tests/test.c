#include "test.h"

#include <stdbool.h>
#include <stdio.h>

#include "file_manager.h"

int success, cnt, allSuccess, allCnt;
bool testFlag;
char* testName;

void TEST_FILE_MANAGER();
void TEST_BPT();
void TEST_FILE();
void TESTS();

int main()
{
    void (*tests[])() = { TEST_FILE_MANAGER, TEST_BPT, TEST_FILE, TESTS };

    char*(testNames[]) = { "file_manager", "bpt", "test_file", "TESTS" };

    for (int i = 0; i < (sizeof(tests) / sizeof(void (*)(void))); ++i)
    {
        success = cnt = 0;

        printf("[%d| %s START]\n", i, testNames[i]);
        tests[i]();
        printf("[success: %d / %d]\n[%d| %s END]\n\n", success, cnt, i,
               testNames[i]);
    }

    printf("\n[Tests are over] success: %d / %d\n", allSuccess, allCnt);
    return 0;
}

void TEST_FILE_MANAGER()
{
    TEST("FmCreate")
    {
        CHECK_TRUE(FmCreate());
    }
    END()

    TEST("FmInit")
    {
        struct FileManager* fm = FmCreate();
        bool result = FmInit(fm);

        CHECK_TRUE(result);
        CHECK_NULL(fm->fp);
        CHECK_VALUE(fm->fileHeader.freePageNumber, 0);
        CHECK_VALUE(fm->fileHeader.numberOfPages, 0);
        CHECK_VALUE(fm->fileHeader.rootPageNumber, 0);

        for (int i = 0; i < 104; ++i)
        {
            CHECK_VALUE(fm->fileHeader.reserved[i], 0);
        }

        CHECK_FALSE(FmInit(NULL));
    }
    END()

    TEST("FmOpen")
    {
        struct FileManager* fm = FmCreate();
        FmInit(fm);
        bool result = FmOpen(fm, "test.db");
        CHECK_TRUE(result);

        CHECK_VALUE(fm->fileHeader.freePageNumber, 0);
        CHECK_VALUE(fm->fileHeader.numberOfPages, 0);
        CHECK_VALUE(fm->fileHeader.rootPageNumber, 0);

        for (int i = 0; i < 104; ++i)
        {
            CHECK_VALUE(fm->fileHeader.reserved[i], 0);
        }
    }
    END()

    TEST("FmPage w/r")
    {
        struct FileManager* fm = FmCreate();
        bool initResult = FmInit(fm);
        CHECK_TRUE(initResult);
        bool openResult = FmOpen(fm, "test.db");
        CHECK_TRUE(openResult);

        struct page_t page;
        for (int i = 0; i < 248; ++i)
        {
            page.entry.internals[i].key = i;
            page.entry.internals[i].pageNumber = i;
        }
        FmPageWrite(fm, 0, &page);

        struct page_t readPage;
        FmPageRead(fm, 0, &readPage);

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

    TEST("CHECK_NULL")
    {
        CHECK_NULL(NULL);
    }
    END()
}
