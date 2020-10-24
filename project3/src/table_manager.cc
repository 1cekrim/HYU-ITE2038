#include "table_manager.hpp"

#include "logger.hpp"

int TableManager::open_table(const std::string& name)
{
    int id;
    if (!free_ids.empty())
    {
        id = free_ids.front();
        free_ids.pop();
        tables[id] = std::make_unique<table_t>();
    }
    else
    {
        id = tables.size();
        tables.emplace_back(new table_t());
    }
    tables[id]->table_id = id;
    CHECK_WITH_LOG(tables[id]->tree.open_table(name), INVALID_TABLE_ID,
                   "open table failure: %s", name.c_str());
    return id;
}

bool TableManager::close_table(int table_id)
{
    // TODO: bptree 닫기 구현
    auto& table = tables.at(table_id);
    table.reset();
    free_ids.emplace(table_id);
    return true;
}

bool TableManager::insert(int table_id, keyType key, const valType& value)
{
    auto& table = tables.at(table_id);
    return table->tree.insert(key, value);
}

bool TableManager::delete_key(int table_id, keyType key)
{
    auto& table = tables.at(table_id);
    return table->tree.delete_key(key);
}

bool TableManager::find(int table_id, keyType key, record_t& ret)
{
    auto& table = tables.at(table_id);
    return table->tree.find(key, ret);
}