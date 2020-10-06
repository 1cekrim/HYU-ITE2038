#ifndef __FILE_H__
#define __FILE_H__
#include <stdint.h>

#define PAGESIZE = 4096

typedef uint64_t pagenum_t;

struct NodePageHeader
{
    pagenum_t parentPageNumber;
    uint32_t isLeaf;
    uint32_t nubmerOfKeys;
    uint8_t reserved[104];
    pagenum_t onePageNumber;
};

struct HeaderPageHeader
{
    pagenum_t freePageNumber;
    pagenum_t rootPageNumber;
    pagenum_t numberOfPages;
    uint8_t reserved[sizeof(struct NodePageHeader) - 24];
};

struct FreePageHeader
{
    pagenum_t nextFreePageNumber;
    uint8_t reserved[sizeof(struct NodePageHeader) - 8];
};

struct Record
{
    int64_t key;
    uint8_t value[120];
};

struct Internal
{
    int64_t key;
    pagenum_t pageNumber;
};

// page
struct page_t
{
    union
    {
        struct NodePageHeader nodePageHeader;
        struct HeaderPageHeader headerPageHeader;
        struct FreePageHeader freePageHeader;
    } header;

    union
    {
        struct Record records[31];
        struct Internal internals[248];
    } entry;
};

pagenum_t file_alloc_page();

void file_free_page(pagenum_t pagenum);

void file_read_page(pagenum_t pagenum, struct page_t* dest);

void file_write_page(pagenum_t pagenum, const struct page_t* src);


#endif /* __FILE_H__*/