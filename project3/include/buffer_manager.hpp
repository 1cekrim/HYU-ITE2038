#ifndef __BUFFER_MANAGER_HPP__
#define __BUFFER_MANAGER_HPP__

#include <unistd.h>

#include <cstdio>
#include <memory>
#include <string_view>
#include <vector>

#include "file_manager.hpp"
#include "frame.hpp"

class BufferManager
{
 public:
    BufferManager();
    ~BufferManager();

    // node manager interface
    bool open(const std::string& name);
    bool commit(pagenum_t pagenum, const frame_t& frame);
    bool load(pagenum_t pagenum, frame_t& frame);
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