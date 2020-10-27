#include "table_manager.hpp"
#include "logger.hpp"

bool TableManager::init_db(int num_buf)
{
    return BufferController::instance().init_buffer_size(num_buf);
}

int TableManager::get_table_id(const std::string& name)
{
    static auto counter = 0;
    if (name_id_table.find(name) == name_id_table.end())
    {
        return name_id_table[name];
    }
    if (counter == MAX_TABLE_NUM)
    {
        return INVALID_TABLE_ID;
    }
    name_id_table[name] = ++counter;
    return counter;
}

int TableManager::open_table(const std::string& name)
{
    if (tables.size() == MAX_TABLE_NUM)
    {
        return INVALID_TABLE_ID;
    }

    int id = get_table_id(name);
    tables[id] = std::make_unique<table_t>();
    tables[id]->table_id = id;
    CHECK_WITH_LOG(tables[id]->tree.open_table(name), INVALID_TABLE_ID,
                   "open table failure: %s", name.c_str());
    return id;
}

bool TableManager::close_table(int table_id)
{
    if (tables.find(table_id) == tables.end())
    {
        return false;
    }
    // TODO: bptree 닫기 구현
    tables.erase(table_id);
    return true;
}

bool TableManager::insert(int table_id, keyType key, const valType& value)
{
    if (tables.find(table_id) == tables.end())
    {
        return false;
    }
    return tables[table_id]->tree.insert(key, value);
}

bool TableManager::delete_key(int table_id, keyType key)
{
    if (tables.find(table_id) == tables.end())
    {
        return false;
    }
    return tables[table_id]->tree.delete_key(key);
}

bool TableManager::find(int table_id, keyType key, record_t& ret)
{
    if (tables.find(table_id) == tables.end())
    {
        return false;
    }
    return tables[table_id]->tree.find(key, ret);
}