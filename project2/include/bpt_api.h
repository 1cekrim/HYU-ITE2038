#include <stdint.h>

extern "C"
int open_table(char* pathname);

extern "C"
int db_insert(int64_t key, char* value);

extern "C"
int db_find(int64_t key, char* ret_val);

extern "C"
int db_delete(int64_t key);