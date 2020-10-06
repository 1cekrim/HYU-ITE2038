#ifndef __TEST_H__
#define __TEST_H__

#define TEST(name)        \
    do                    \
    {                     \
        testName = name;  \
        testFlag = false; \
        printf("  [%s]...", testName);

#define END()             \
    }                     \
    while (0)             \
        ;                 \
    ++cnt;                \
    ++allCnt;             \
    if (!testFlag)        \
    {                     \
        puts(" success"); \
        ++success;        \
        ++allSuccess;     \
    }                     \
    else                  \
    {                     \
    }

#define CHECK_TRUE(expression)                      \
    if (!(expression))                              \
    {                                               \
        printf(" [ (%s) != true ]\n", #expression); \
        testFlag = true;                            \
        break;                                      \
    }

#define CHECK_FALSE(expression)                     \
    if (expression)                                 \
    {                                               \
        printf(" [ (%s) != true ]\n", #expression); \
        testFlag = true;                            \
        break;                                      \
    }

#define CHECK_VALUE(expression, value)                     \
    if ((expression) != (value))                           \
    {                                                      \
        printf(" [ (%s) != %d ]\n", #expression, (value)); \
        testFlag = true;                                   \
        break;                                             \
    }

#define CHECK_NULL(expression)                      \
    if (expression != NULL)                         \
    {                                               \
        printf(" [ (%s) != NULL ]\n", #expression); \
        testFlag = true;                            \
        break;                                      \
    }

#endif /* __TEST_H__*/