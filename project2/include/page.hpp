#ifndef __PAGE_HPP__
#define __PAGE_HPP__

#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>

class FileManager;

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

    NodePageHeader()
        : parentPageNumber(0),
          isLeaf(0),
          numberOfKeys(0),
          reserved(),
          onePageNumber(0)
    {
        // Do nothing
    }

    NodePageHeader(const NodePageHeader& header)
        : parentPageNumber(header.parentPageNumber),
          isLeaf(header.isLeaf),
          numberOfKeys(header.numberOfKeys),
          reserved(),
          onePageNumber(header.onePageNumber)
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

    HeaderPageHeader()
        : freePageNumber(0), rootPageNumber(0), numberOfPages(0), reserved()
    {
        // Do nothing
    }

    HeaderPageHeader(const HeaderPageHeader& header)
        : freePageNumber(header.freePageNumber),
          rootPageNumber(header.rootPageNumber),
          numberOfPages(header.numberOfPages)
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

    FreePageHeader(const FreePageHeader& header)
        : nextFreePageNumber(header.nextFreePageNumber), reserved()
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
    pagenum_t node_id;
    Internal& operator=(const Internal& rhs)
    {
        memcpy(this, &rhs, sizeof(Internal));
        return *this;
    }
    void init(keyType key, pagenum_t node_id)
    {
        this->key = key;
        this->node_id = node_id;
    }
};

struct Records
{
    std::array<Record, 31> records;
    constexpr auto begin()
    {
        return std::begin(records);
    }
    Record& operator[](int idx)
    {
        return const_cast<Record&>(std::as_const(*this)[idx]);
    }
    const Record& operator[](int idx) const
    {
        return records[idx];
    }
};

struct Internals
{
    std::array<Internal, 248> internals;
    constexpr auto begin()
    {
        return std::begin(internals);
    }
    Internal& operator[](int idx)
    {
        return const_cast<Internal&>(std::as_const(*this)[idx]);
    }

    const Internal& operator[](int idx) const
    {
        return internals[idx];
    }
};

template <typename It>
struct range_t
{
    It b, e;
    constexpr It begin() const
    {
        return b;
    }
    constexpr It end() const
    {
        return e;
    }
    constexpr std::size_t size() const
    {
        return end() - begin();
    }
    bool empty() const
    {
        return begin() == end();
    }
    decltype(auto) front() const
    {
        return *begin();
    }
    decltype(auto) back() const
    {
        return *(std::prev(end()));
    }
    range_t(It b, It e) : b(b), e(e)
    {
        // Do nothing
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
        Records records;
        Internals internals;
    } entry;

    template <typename T>
    const T& getEntry() const;

    template <typename T>
    T& getEntry();

    template <typename T>
    const T& getHeader() const;

    template <typename T>
    T& getHeader();

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

    auto& records()
    {
        return entry.records;
    }

    auto& internals()
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

    const auto& records() const
    {
        return entry.records;
    }

    const auto& internals() const
    {
        return entry.internals;
    }

    bool commit(FileManager& manager, pagenum_t pagenum) const;
    bool load(FileManager& manager, pagenum_t pagenum);

    template <typename T>
    void push_back(const T& value)
    {
        static_assert(std::is_same<Record, T>::value ||
                      std::is_same<Internal, T>::value);
        using S = typename std::conditional<std::is_same<Record, T>::value,
                                            Records, Internals>::type;
        getEntry<S>()[static_cast<int>(
            getHeader<NodePageHeader>().numberOfKeys++)] = value;
    }

    template <typename T, typename K, typename B>
    void emplace_back(const K& key, const B& v)
    {
        static_assert(std::is_same<Record, T>::value ||
                      std::is_same<Internal, T>::value);

        if constexpr (std::is_same<Record, T>::value)
        {
            auto& entry = getEntry<Records>()[static_cast<int>(
                getHeader<NodePageHeader>().numberOfKeys++)];
            entry.key = key;
            entry.value = v;
        }
        else
        {
            auto& entry = getEntry<Internals>()[static_cast<int>(
                getHeader<NodePageHeader>().numberOfKeys++)];
            entry.key = key;
            entry.node_id = v;
        }
    }

    template <typename T>
    void insert(const T& entry, int insertion_point)
    {
        static_assert(std::is_same<Record, T>::value ||
                      std::is_same<Internal, T>::value);
        auto& header = nodePageHeader();
        using S = typename std::conditional<std::is_same<Record, T>::value,
                                            Records, Internals>::type;

        S& entries = getEntry<S>();

        for (int i = header.numberOfKeys; i > insertion_point; --i)
        {
            entries[i] = entries[i - 1];
        }

        ++header.numberOfKeys;
        entries[insertion_point] = entry;
    }

    template <typename T>
    void erase(int erase_point)
    {
        static_assert(std::is_same<Record, T>::value ||
                      std::is_same<Internal, T>::value);
        auto& header = nodePageHeader();
        using S = typename std::conditional<std::is_same<Record, T>::value,
                                            Records, Internals>::type;

        S& entries = getEntry<S>();

        for (int i = erase_point + 1; i < static_cast<int>(header.numberOfKeys);
             ++i)
        {
            entries[i - 1] = entries[i];
        }

        --header.numberOfKeys;
    }

    template <typename T>
    decltype(auto) range(std::size_t begin = 0, std::size_t end = 0)
    {
        static_assert(std::is_same<Record, T>::value ||
                      std::is_same<Internal, T>::value);
        if (end == 0)
        {
            end = getHeader<NodePageHeader>().numberOfKeys;
        }

        using S = typename std::conditional<std::is_same<Record, T>::value,
                                            Records, Internals>::type;
        return range_t(std::next(std::begin(getEntry<S>()), begin),
                       std::next(std::begin(getEntry<S>()), end));
    }

    template <typename T, typename It>
    void range_copy(It it, int begin = 0, int end = -1)
    {
        static_assert(std::is_same<Record, T>::value ||
                      std::is_same<Internal, T>::value);
        if (end == -1)
        {
            end = getHeader<NodePageHeader>().numberOfKeys;
        }
        using S = typename std::conditional<std::is_same<Record, T>::value,
                                            Records, Internals>::type;
        std::copy(std::next(std::begin(getEntry<S>()), begin),
                  std::next(std::begin(getEntry<S>()), end), it);
    }

    template <typename T, typename It>
    void range_assignment(It begin, It end)
    {
        static_assert(std::is_same<Record, T>::value ||
                      std::is_same<Internal, T>::value);
        using S = typename std::conditional<std::is_same<Record, T>::value,
                                            Records, Internals>::type;
        std::copy(begin, end, std::begin(getEntry<S>()));
        getHeader<NodePageHeader>().numberOfKeys = std::distance(begin, end);
    }

    template <typename T>
    int satisfy_condition_first(std::function<bool(const T&)> condition) const
    {
        static_assert(std::is_same<Record, T>::value ||
                      std::is_same<Internal, T>::value);
        using S = typename std::conditional<std::is_same<Record, T>::value,
                                            Records, Internals>::type;
        int index{};
        auto n = static_cast<int>(getHeader<NodePageHeader>().numberOfKeys);
        while (index < n && !condition(getEntry<S>()[index]))
        {
            ++index;
        }
        return index;
    }

    template <typename T>
    T& get(std::size_t idx)
    {
        static_assert(std::is_same<Record, T>::value ||
                      std::is_same<Internal, T>::value);
        using S = typename std::conditional<std::is_same<Record, T>::value,
                                            Records, Internals>::type;
        return getEntry<S>()[idx];
    }

    void print_node();

    int number_of_keys() const;
    pagenum_t parent() const;
    void set_parent(pagenum_t parent);
    bool is_leaf() const;

    pagenum_t leftmost() const;
    void set_leftmost(pagenum_t leftmost);
    pagenum_t next_leaf() const;
    void set_next_leaf(pagenum_t next_leaf);
};

#endif /* __PAGE_HPP__*/