#ifndef __PAGE_HPP__
#define __PAGE_HPP__

#include <cstdint>
#include <cstring>
#include <memory>
#include <iostream>

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
    uint8_t reserved[104];
    pagenum_t onePageNumber;

    friend std::ostream& operator<<(std::ostream& os,
                                    const NodePageHeader& nph);
};

struct HeaderPageHeader
{
    pagenum_t freePageNumber;
    pagenum_t rootPageNumber;
    pagenum_t numberOfPages;
    uint8_t reserved[sizeof(struct NodePageHeader) - 24];
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
    uint8_t reserved[sizeof(struct NodePageHeader) - 8];
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
    union
    {
        HeaderPageHeader headerPageHeader;
        NodePageHeader nodePageHeader;
        FreePageHeader freePageHeader;
    } header;

    union
    {
        Record records[31];
        Internal internals[248];
    } entry;

    void insert_record(const Record& record, int insertion_point)
    {
        auto& head = header.nodePageHeader;
        auto& rec = entry.records;

        for (int i = head.numberOfKeys; i > insertion_point; --i)
        {
            rec[i] = rec[i - 1];
        }

        ++head.numberOfKeys;
        rec[insertion_point] = record;
    }

    void insert_internal(const Internal& internal, int insertion_point)
    {
        auto& head = header.nodePageHeader;
        auto& internals = entry.internals;

        for (int i = head.numberOfKeys; i > insertion_point; --i)
        {
            internals[i] = internals[i - 1];
        }

        ++head.numberOfKeys;
        internals[insertion_point] = internal;
    }

    void push_record(const Record& record)
    {
        entry.records[header.nodePageHeader.numberOfKeys++] = record;
    }

    void push_internal(const Internal& internal)
    {
        entry.internals[header.nodePageHeader.numberOfKeys++] = internal;
    }

    void print_node();
};

#endif /* __PAGE_HPP__*/