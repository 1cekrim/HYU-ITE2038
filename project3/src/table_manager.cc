#include "table_manager.hpp"

#include "logger.hpp"

bool TableManager::init_db(int num_buf)
{
    CHECK_WITH_LOG(!valid_table_manager, false, "shutdown first");
    CHECK_WITH_LOG(BufferController::instance().init_buffer(num_buf), false,
                   "init buffer failure");
    valid_table_manager = true;
    return true;
}

bool TableManager::shutdown_db()
{
    CHECK_WITH_LOG(valid_table_manager, false, "init first");
    valid_table_manager = false;
    CHECK_WITH_LOG(BufferController::instance().clear_buffer(), false,
                   "clear buffer failure");
    tables.clear();
    name_id_table.clear();
    return true;
}

int TableManager::get_table_id(const std::string& name)
{
    if (!valid_table_manager)
    {
        return INVALID_TABLE_ID;
    }
    auto counter = name_id_table.size();
    if (name_id_table.find(name) != name_id_table.end())
    {
        return name_id_table[name];
    }
    if (counter == MAX_TABLE_NUM)
    {
        return INVALID_TABLE_ID;
    }
    name_id_table[name] = counter + 1;
    return counter;
}

int TableManager::open_table(const std::string& name)
{
    if (!valid_table_manager)
    {
        return INVALID_TABLE_ID;
    }

    int id = get_table_id(name);
    if (id == INVALID_TABLE_ID)
    {
        return INVALID_TABLE_ID;
    }
    
    tables[id] = std::make_unique<table_t>();
    tables[id]->table_id = id;
    CHECK_WITH_LOG(tables[id]->tree.open_table(name), INVALID_TABLE_ID,
                   "open table failure: %s", name.c_str());
    return id;
}

bool TableManager::close_table(int table_id)
{
    if (!valid_table_manager || tables.find(table_id) == tables.end())
    {
        return false;
    }
    tables.erase(table_id);
    return true;
}

bool TableManager::insert(int table_id, keyType key, const valType& value)
{
    if (!valid_table_manager || tables.find(table_id) == tables.end())
    {
        return false;
    }
    return tables[table_id]->tree.insert(key, value);
}

bool TableManager::delete_key(int table_id, keyType key)
{
    if (!valid_table_manager || tables.find(table_id) == tables.end())
    {
        return false;
    }
    return tables[table_id]->tree.delete_key(key);
}

bool TableManager::find(int table_id, keyType key, record_t& ret)
{
    if (!valid_table_manager || tables.find(table_id) == tables.end())
    {
        return false;
    }
    return tables[table_id]->tree.find(key, ret);
}