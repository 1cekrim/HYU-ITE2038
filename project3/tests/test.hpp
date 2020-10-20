#ifndef __TEST_H__
#define __TEST_H__

#define TEST(name)                                \
    do                                            \
    {                                             \
        testName = name;                          \
        testFlag = false;                         \
        std::cout << "  [" << testName << "]..."; \
        std::cout.flush();

#define END()                      \
    }                              \
    while (0)                      \
        ;                          \
    ++cnt;                         \
    ++allCnt;                      \
    if (!testFlag)                 \
    {                              \
        std::cout << " success\n"; \
        ++success;                 \
        ++allSuccess;              \
    }                              \
    else                           \
    {                              \
    }

#define CHECK_TRUE(expression)                                \
    if (!(expression))                                        \
    {                                                         \
        std::cout << " [ (" << #expression << " != true ]\n"; \
        testFlag = true;                                      \
        break;                                                \
    }

#define CHECK_FALSE(expression)                                \
    if (expression)                                            \
    {                                                          \
        std::cout << " [ (" << #expression << " != false ]\n"; \
        testFlag = true;                                       \
        break;                                                 \
    }

#define CHECK_VALUE(expression, value)                                     \
    if ((expression) != (value))                                           \
    {                                                                      \
        std::cout << " [ (" << #expression << ") != " << (value) << "]\n"; \
        testFlag = true;                                                   \
        break;                                                             \
    }

#define CHECK_NULLPTR(expression)                                \
    if (expression != NULL)                                      \
    {                                                            \
        std::cout << " [ (" << #expression << " != nullptr ]\n"; \
        testFlag = true;                                         \
        break;                                                   \
    }

#endif /* __TEST_H__*/