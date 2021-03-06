#ifndef __LOGGER_HPP__
#define __LOGGER_HPP__

#include <cstdio>

#define S_COLOR_RED "\x1b[31m"
#define S_COLOR_GREEN "\x1b[32m"
#define S_COLOR_YELLOW "\x1b[33m"
#define S_COLOR_BLUE "\x1b[34m"
#define S_COLOR_MAGENTA "\x1b[35m"
#define S_COLOR_CYAN "\x1b[36m"
#define S_COLOR_RESET "\x1b[0m"

#define CHECK_WITH_LOG(statement, ret_val, fmt, args...)       \
    do                                                         \
    {                                                          \
        if (!(statement))                                      \
        {                                                      \
            printf(S_COLOR_RED "[-] %s", __PRETTY_FUNCTION__); \
            printf("\n    statement: " #statement "\n    " fmt \
                   "\n" S_COLOR_RESET,                         \
                   ##args);                                    \
            return ret_val;                                    \
        }                                                      \
    } while (0)

#define EXIT_WITH_LOG(statement, fmt, args...)                 \
    do                                                         \
    {                                                          \
        if (!(statement))                                      \
        {                                                      \
            printf(S_COLOR_RED "[-] %s", __PRETTY_FUNCTION__); \
            printf("\n    statement: " #statement "\n    " fmt \
                   "\n" S_COLOR_RESET,                         \
                   ##args);                                    \
            exit(-1);                                          \
        }                                                      \
    } while (0)

#define CHECK(statement)                                       \
    do                                                         \
    {                                                          \
        if (!(statement))                                      \
        {                                                      \
            printf(S_COLOR_RED "[-] %s", __PRETTY_FUNCTION__); \
            printf("\n    statement: " #statement "\n");       \
            return false;                                      \
        }                                                      \
    } while (0)
#define CHECK_RET(statement, ret)                              \
    do                                                         \
    {                                                          \
        if (!(statement))                                      \
        {                                                      \
            printf(S_COLOR_RED "[-] %s", __PRETTY_FUNCTION__); \
            printf("\n    statement: " #statement "\n");       \
            return (ret);                                      \
        }                                                      \
    } while (0)

#endif /* __LOGGER_HPP__*/