#include "file_api.hpp"

#include "file_manager.hpp"
#include "logger.hpp"

extern "C" {
pagenum_t file_alloc_page()
{
    auto fm = FileManager::lastOpenedFileManager;
    return fm->create();
}

void file_free_page(pagenum_t pagenum)
{
    auto fm = FileManager::lastOpenedFileManager;
    EXIT_WITH_LOG(fm->free(pagenum), "file_free_page failure. pagenum: %ld",
                  pagenum);
}

void file_read_page(pagenum_t pagenum, page_t* dest)
{
    auto fm = FileManager::lastOpenedFileManager;
    EXIT_WITH_LOG(fm->load(pagenum, *dest),
                  "file_read_page failure. pagenum: %ld", pagenum);
}

void file_write_page(pagenum_t pagenum, const page_t* src)
{
    auto fm = FileManager::lastOpenedFileManager;
    EXIT_WITH_LOG(fm->commit(pagenum, *src),
                  "file_write_page failure. pagenum: %ld", pagenum);
}
}