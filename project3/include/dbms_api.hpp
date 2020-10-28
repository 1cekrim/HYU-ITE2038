#ifndef __DBMS_API_HPP__
#define __DBMS_API_HPP__

#include <stdint.h>

extern "C" {

int init_db(int buf_num);

int open_table(char* pathname);

int db_insert(int table_id, int64_t key, char* value);

int db_find(int table_id, int64_t key, char* ret_val);

int db_delete(int table_id, int64_t key);

int close_table(int table_id);

int shutdown_db();
}

#endif /* __DBMS_API_HPP__*/