#ifndef __TABLE_MANAGER_HPP__
#define __TABLE_MANAGER_HPP__

#include <queue>

#include "bptree.hpp"
#include "page.hpp"

struct table_t;

constexpr auto INVALID_TABLE_ID = -1;

class TableManager
{
 public:
    static TableManager& instance()
    {
        static TableManager tm;
        return tm;
    }

    int open_table(const std::string& name);
    bool close_table(int table_id);
    bool insert(int table_id, keyType key, const valType& value);
    bool delete_key(int table_id, keyType key);
    bool find(int table_id, keyType key, record_t& ret);
    static void char_to_valType(valType& dst, const char* src)
    {
        std::fill(std::begin(dst), std::end(dst), 0);
        std::copy_n(src, std::min(static_cast<int>(strlen(src)), 119),
                    std::begin(dst));
    }

 private:
    TableManager()
    {
        // Do nothing
    }
    std::vector<std::unique_ptr<table_t>> tables;
    std::queue<int> free_ids;
};

struct table_t
{
    BPTree tree;
    int table_id;
};

#endif /* __TABLE_MANAGER_HPP__*/