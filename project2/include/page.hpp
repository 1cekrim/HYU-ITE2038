#ifndef __PAGE_HPP__
#define __PAGE_HPP__

#include <array>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>

#define PAGESIZE 4096

constexpr auto value_size = 120;
using valType = uint8_t[120];
using keyType = int64_t;
using pagenum_t = uint64_t;

constexpr auto EMPTY_PAGE_NUMBER = 0;

struct NodePageHeader
{
    pagenum_t parentPageNumber;
    uint32_t isLeaf;
    uint32_t numberOfKeys;
    std::array<uint8_t, 104> reserved;
    pagenum_t onePageNumber;

    NodePageHeader() : parentPageNumber(0), isLeaf(0), numberOfKeys(0), reserved(), onePageNumber(0)
    {
        // Do nothing
    }

    NodePageHeader(const NodePageHeader& header) : parentPageNumber(header.parentPageNumber), isLeaf(header.isLeaf), numberOfKeys(header.numberOfKeys), reserved(), onePageNumber(header.onePageNumber)
    {
        // Do nothing
    }

    NodePageHeader& operator=(const NodePageHeader& header)
    {
        parentPageNumber = header.parentPageNumber;
        isLeaf = header.isLeaf;
        numberOfKeys = header.numberOfKeys;
        onePageNumber = header.onePageNumber;
        return *this;
    }

    friend std::ostream& operator<<(std::ostream& os,
                                    const NodePageHeader& nph);
};

struct HeaderPageHeader
{
    pagenum_t freePageNumber;
    pagenum_t rootPageNumber;
    pagenum_t numberOfPages;
    std::array<uint8_t, sizeof(struct NodePageHeader) - 24> reserved;

    HeaderPageHeader() : freePageNumber(0), rootPageNumber(0), numberOfPages(0), reserved()
    {
        // Do nothing
    }

    HeaderPageHeader(const HeaderPageHeader& header) : freePageNumber(header.freePageNumber), rootPageNumber(header.rootPageNumber), numberOfPages(header.numberOfPages)
    {
        // Do nothing
    }

    HeaderPageHeader& operator=(const HeaderPageHeader& header)
    {
        freePageNumber = header.freePageNumber;
        rootPageNumber = header.rootPageNumber;
        numberOfPages = header.numberOfPages;
        return *this;
    }
    friend std::ostream& operator<<(std::ostream& os,
                                    const HeaderPageHeader& hph);
};

struct FreePageHeader
{
    pagenum_t nextFreePageNumber;
    std::array<uint8_t, sizeof(struct NodePageHeader) - 8> reserved;
    
    FreePageHeader() : nextFreePageNumber(0), reserved()
    {
        // Do nothing
    }

    FreePageHeader(const FreePageHeader& header) : nextFreePageNumber(header.nextFreePageNumber), reserved()
    {
        // Do nothing
    }
};

struct Record
{
    keyType key;
    valType value;
    Record& operator=(const Record& rhs)
    {
        memcpy(this, &rhs, sizeof(Record));
        return *this;
    }
    void init(keyType key, const valType& val)
    {
        this->key = key;
        memcpy(&value, &val, sizeof(valType));
    }
};

struct Internal
{
    keyType key;
    pagenum_t pageNumber;
    Internal& operator=(const Internal& rhs)
    {
        memcpy(this, &rhs, sizeof(Internal));
        return *this;
    }
    void init(keyType key, pagenum_t pageNumber)
    {
        this->key = key;
        this->pageNumber = pageNumber;
    }
};

// page
struct page_t
{
    union Header
    {
        HeaderPageHeader headerPageHeader;
        NodePageHeader nodePageHeader;
        FreePageHeader freePageHeader;
        Header() : freePageHeader()
        {
            // Do nothing
        }
    } header;

    union Entry
    {
        std::array<Record, 31> records;
        std::array<Internal, 248> internals;
    } entry;

    HeaderPageHeader& headerPageHeader()
    {
        return header.headerPageHeader;
    }

    NodePageHeader& nodePageHeader()
    {
        return header.nodePageHeader;
    }

    FreePageHeader& freePageHeader()
    {
        return header.freePageHeader;
    }

    std::array<Record, 31>& records()
    {
        return entry.records;
    }

    std::array<Internal, 248>& internals()
    {
        return entry.internals;
    }

    const HeaderPageHeader& headerPageHeader() const
    {
        return header.headerPageHeader;
    }

    const NodePageHeader& nodePageHeader() const
    {
        return header.nodePageHeader;
    }

    const FreePageHeader& freePageHeader() const
    {
        return header.freePageHeader;
    }

    const std::array<Record, 31>& records() const
    {
        return entry.records;
    }

    const std::array<Internal, 248>& internals() const
    {
        return entry.internals;
    }

    void insert_record(const Record& record, int insertion_point)
    {
        auto& head = nodePageHeader();
        auto& rec = records();

        for (int i = head.numberOfKeys; i > insertion_point; --i)
        {
            rec[i] = rec[i - 1];
        }

        ++head.numberOfKeys;
        rec[insertion_point] = record;
    }

    void insert_internal(const Internal& internal, int insertion_point)
    {
        auto& head = nodePageHeader();
        auto& internals = this->internals();

        for (int i = head.numberOfKeys; i > insertion_point; --i)
        {
            internals[i] = internals[i - 1];
        }

        ++head.numberOfKeys;
        internals[insertion_point] = internal;
    }

    void push_record(const Record& record)
    {
        records()[nodePageHeader().numberOfKeys++] = record;
    }

    void push_internal(const Internal& internal)
    {
        internals()[nodePageHeader().numberOfKeys++] = internal;
    }

    void print_node();
};

#endif /* __PAGE_HPP__*/