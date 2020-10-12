#include "file_api.hpp"

extern "C" {
pagenum_t file_alloc_page()
{
    return EMPTY_PAGE_NUMBER;
}

void file_free_page(pagenum_t pagenum)
{
    
}

void file_read_page(pagenum_t pagenum, page_t* dest)
{
    
}

void file_write_page(pagenum_t pagenum, const page_t* src)
{
    
}
}