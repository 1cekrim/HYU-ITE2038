#ifndef __FILE_H__
#define __FILE_H__

#include <stdint.h>

typedef uint64_t pagenum_t;

struct page_t;

pagenum_t file_alloc_page();

void file_free_page(pagenum_t pagenum);

void file_read_page(pagenum_t pagenum, struct page_t* dest);

void file_write_page(pagenum_t pagenum, const struct page_t* src);
#endif /* __FILE_H__*/