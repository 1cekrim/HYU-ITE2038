#ifndef __FILE_API_HPP__
#define __FILE_API_HPP__

#include "page.hpp"


pagenum_t file_alloc_page();

void file_free_page(pagenum_t pagenum);

void file_read_page(pagenum_t pagenum, page_t* dest);

void file_write_page(pagenum_t pagenum, const page_t* src);

#endif /* __FILE_API_HPP__*/