#ifndef __BUFFER_MANAGER_HPP__
#define __BUFFER_MANAGER_HPP__

#include <unistd.h>

#include <cstdio>
#include <memory>
#include <string_view>
#include <vector>

#include "page.hpp"
#include "file_manager.hpp"

class BufferManager
{
 public:
    BufferManager();
    ~BufferManager();

    // node manager interface
    bool open(const std::string& name);
    bool commit(pagenum_t pagenum, const page_t& page);
    bool load(pagenum_t pagenum, page_t& page);
    pagenum_t create();
    bool free(pagenum_t pagenum);
    int get_manager_id() const;
    pagenum_t root() const;
    bool set_root(pagenum_t pagenum);

    const HeaderPageHeader& getFileHeader() const;

 private:
    FileManager fileManager;
};

#endif /* __BUFFER_MANAGER_HPP__*/