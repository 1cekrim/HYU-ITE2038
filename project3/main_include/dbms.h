#ifndef __DBMS_H__
#define __DBMS_H__
#include <stdint.h>

int open_table(char* pathname);

int db_insert(int64_t key, char* value);

int db_find(int64_t key, char* ret_val);

int db_delete(int64_t key);
#endif /* __DBMS_H__*/