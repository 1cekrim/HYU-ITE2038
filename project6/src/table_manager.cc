#include "table_manager.hpp"

#include "logger.hpp"
#include "log_manager.hpp"

bool TableManager::init_db(int buf_num, int flag, int log_num, char* log_path, char* logmsg)
{
    CHECK_WITH_LOG(!valid_table_manager, false, "shutdown first");
    CHECK_WITH_LOG(BufferController::instance().init_buffer(buf_num), false,
                   "init buffer failure");
    valid_table_manager = true;
    std::cout << "init_db open\n";
    LogManager::instance().open(log_path, logmsg);
    std::cout << "init_db recovery\n";
    LogManager::instance().recovery(RecoveryMode(flag), log_num);
    std::cout << "init_db end\n";
    return true;
}

bool TableManager::shutdown_db()
{
    CHECK_WITH_LOG(valid_table_manager, false, "init first");
    valid_table_manager = false;
    LogManager::instance().flush();
    CHECK_WITH_LOG(BufferController::instance().clear_buffer(), false,
                   "clear buffer failure");
    tables.clear();
    name_id_table.clear();
    return true;
}

int TableManager::get_table_id(const std::string& name)
{
    // if (!valid_table_manager)
    // {
    //     return INVALID_TABLE_ID;
    // }
    // auto counter = name_id_table.size();
    // if (name_id_table.find(name) != name_id_table.end())
    // {
    //     return name_id_table[name];
    // }
    // if (counter == MAX_TABLE_NUM)
    // {
    //     return INVALID_TABLE_ID;
    // }
    // name_id_table[name] = ++counter;
    if (name_id_table.find(name) != name_id_table.end())
    {
        return name_id_table[name];
    }

    if (name.substr(0, 4) != "DATA")
    {
        std::cerr << "name format: DATA[file_id]";
        return -1;
    }

    std::string number = name.substr(4, name.size() - 4);
    // std::cout << number << '\n';
    int id;
    try
    {
        id = std::stoi(number);
    }
    catch (std::invalid_argument&)
    {
        std::cerr << "name format: DATA[file_id]";
        return -1;
    }

    name_id_table[name] = id;

    return id;
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
    tables[id]->tree.set_table(id);
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

bool TableManager::update(int table_id, keyType key, const valType& value,
                          int trx_id)
{
    if (!valid_table_manager || tables.find(table_id) == tables.end())
    {
        return false;
    }
    return tables[table_id]->tree.update(key, value, trx_id);
}

bool TableManager::delete_key(int table_id, keyType key)
{
    if (!valid_table_manager || tables.find(table_id) == tables.end())
    {
        return false;
    }
    return tables[table_id]->tree.delete_key(key);
}

bool TableManager::find(int table_id, keyType key, record_t& ret, int trx_id)
{
    if (!valid_table_manager || tables.find(table_id) == tables.end())
    {
        return false;
    }
    return tables[table_id]->tree.find(key, ret, trx_id);
}