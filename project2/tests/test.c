#include <stdbool.h>
#include <stdio.h>

#include "test.h"

int success, cnt;
bool testFlag;
char* testName;

void TEST_FILE_MANAGER();
void TEST_BPT();
void TEST_FILE();
void TESTS();

int main()
{
    void (*tests[])() = {
        TEST_FILE_MANAGER,
        TEST_BPT,
        TEST_FILE,
        TESTS
    };

    char* (testNames[]) = {
        "file_manager",
        "bpt",
        "test_file",
        "TESTS"
    };

    for (int i = 0; i < (sizeof(tests) / sizeof(void (*)(void))); ++i)
    {
        success = cnt = 0;

        printf("[%d| %s START]\n", i, testNames[i]);
        tests[i]();
        printf("[success: %d / %d]\n[%d| %s END]\n\n", success, cnt, i, testNames[i]);
    }
}

void TEST_FILE_MANAGER()
{

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
        CHECK_TRUE(1 == 1);
        CHECK_TRUE(true);
        CHECK_TRUE(2);
        CHECK_TRUE(-1);
    END()

    TEST("CHECK_FALSE")
        CHECK_FALSE(1 == 2);
        CHECK_FALSE(0);
        CHECK_FALSE(false);
    END()
}

