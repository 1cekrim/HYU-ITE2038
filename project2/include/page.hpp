#ifndef __PAGE_HPP__
#define __PAGE_HPP__

#include <cstdint>
#include <cstring>
#include <memory>

#define PAGESIZE 4096

constexpr auto value_size = 120;
using val_t = uint8_t[120];
using keyType = int64_t;
using pagenum_t = uint64_t;

constexpr auto EMPTY_PAGE_NUMBER = 0;

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
    keyType key;
    val_t value;
    Record& operator=(const Record& rhs)
    {
        memcpy(this, &rhs, sizeof(Record));
        return *this;
    }
};

struct Internal
{
    keyType key;
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

    void insert_record(std::unique_ptr<Record> record, int insertion_point)
    {
        auto& head = header.nodePageHeader;
        auto& rec = entry.records;
        
        for (int i = head.nubmerOfKeys; i > insertion_point; --i)
        {
            rec[i] = rec[i - 1];
        }

        ++head.nubmerOfKeys;
        rec[insertion_point] = *record;
    }
};

#endif /* __PAGE_HPP__*/