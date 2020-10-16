#include "dbms_api.hpp"

#include <cstdio>
#include <memory>

#include "bptree.hpp"
#include "file_manager.hpp"

auto bpt = std::make_unique<BPTree>();

extern "C" {
int open_table(char* pathname)
{
    if (!bpt->open_table(pathname))
    {
        return -1;
    }

    return bpt->get_table_id();
}

int db_insert(int64_t key, char* value)
{
    if (bpt->get_table_id() == -1)
    {
        return -1;
    }

    valType v;
    bpt->char_to_valType(v, value);
    if (!bpt->insert(key, v))
    {
        return 1;
    }
    return 0;
}

int db_find(int64_t key, char* ret_val)
{
    if (bpt->get_table_id() == -1)
    {
        return -1;
    }

    auto rec = bpt->find(key);
    if (!rec)
    {
        return 1;
    }

    for (int i = 0; i < value_size; ++i)
    {
        if (!rec->value[i])
        {
            ret_val[i] = '\0';
            break;
        }
        ret_val[i] = rec->value[i];
    }

    return 0;
}

int db_delete(int64_t key)
{
    if (bpt->get_table_id() == -1)
    {
        return -1;
    }

    auto res = bpt->delete_key(key);

    return !res;
}
}
