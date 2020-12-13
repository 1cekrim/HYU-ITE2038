#ifndef __TABLE_MANAGER_HPP__
#define __TABLE_MANAGER_HPP__

#include <queue>
#include <unordered_map>

#include "bptree.hpp"
#include "page.hpp"

struct table_t;

constexpr auto INVALID_TABLE_ID = -1;
constexpr auto MAX_TABLE_NUM = 10;

class TableManager
{
 public:
    static TableManager& instance()
    {
        static TableManager tm;
        return tm;
    }

    bool init_db(int buf_num, int flag, int log_num, char* log_path, char* logmsg);
    bool shutdown_db();
    int open_table(const std::string& name);
    bool close_table(int table_id);
    bool insert(int table_id, keyType key, const valType& value);
    bool update(int table_id, keyType key, const valType& value,
                int trx_id = TransactionManager::invliad_transaction_id);
    bool delete_key(int table_id, keyType key);
    bool find(int table_id, keyType key, record_t& ret,
              int trx_id = TransactionManager::invliad_transaction_id);
    static void char_to_valType(valType& dst, const char* src)
    {
        std::fill(std::begin(dst), std::end(dst), 0);
        std::copy_n(src, std::min(static_cast<int>(strlen(src)), 119),
                    std::begin(dst));
    }

 private:
    std::unordered_map<int, std::unique_ptr<table_t>> tables;
    std::unordered_map<std::string, int> name_id_table;
    bool valid_table_manager;
    TableManager() : valid_table_manager(false)
    {
        // Do nothing
    }
    ~TableManager()
    {
        if (valid_table_manager)
        {
            shutdown_db();
        }
    }
    int get_table_id(const std::string& name);
};

struct table_t
{
    BPTree tree;
    int table_id;
};

#endif /* __TABLE_MANAGER_HPP__*/