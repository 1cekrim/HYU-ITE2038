#ifndef __FILE_H__
#define __FILE_H__
#include <cstdint>
#include <cstring>

#define PAGESIZE 4096

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
    void Init()
    {
        std::memset(this, 0, sizeof(HeaderPageHeader));
    }
    HeaderPageHeader& operator=(const HeaderPageHeader& header)
    {
        freePageNumber = header.freePageNumber;
        rootPageNumber = header.rootPageNumber;
        numberOfPages = header.numberOfPages;
        return *this;
    }
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
        struct HeaderPageHeader headerPageHeader;
        struct NodePageHeader nodePageHeader;
        struct FreePageHeader freePageHeader;
    } header;

    union
    {
        struct Record records[31];
        struct Internal internals[248];
    } entry;

    page_t()
    {
        std::memset(this, 0, sizeof(page_t));
    }
};

pagenum_t file_alloc_page();

void file_free_page(pagenum_t pagenum);

void file_read_page(pagenum_t pagenum, page_t& dest);

void file_write_page(pagenum_t pagenum, const page_t& src);

void InitHeaderPage(page_t& page, const HeaderPageHeader& HeaderPageHeader);

#endif /* __FILE_H__*/