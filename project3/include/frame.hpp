#ifndef __FRAME_HPP__
#define __FRAME_HPP__

#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>

class FileManager;

#define PAGESIZE 4096

constexpr auto value_size = 120;
using valType = std::array<uint8_t, 120>;
using keyType = int64_t;
using pagenum_t = uint64_t;

constexpr auto EMPTY_PAGE_NUMBER = 0;

// frame
struct frame_t
{

    // page_t& operator=(const page_t& rhs)
    // {
    //     std::memcpy(this, &rhs, sizeof(page_t));
    //     return *this;
    // }

    // page_t() = default;
    // page_t(bool isLeaf)
    // {
    //     header.nodePageHeader.isLeaf = isLeaf;
    // }
    // page_t(const page_t& p) = default;

    // template <typename T>
    // const T& getEntry() const;

    // template <typename T>
    // T& getEntry();

    // template <typename T>
    // const T& getHeader() const;

    // template <typename T>
    // T& getHeader();

    // HeaderPageHeader& headerPageHeader()
    // {
    //     return header.headerPageHeader;
    // }

    // NodePageHeader& nodePageHeader()
    // {
    //     return header.nodePageHeader;
    // }

    // FreePageHeader& freePageHeader()
    // {
    //     return header.freePageHeader;
    // }

    // auto& records()
    // {
    //     return entry.records;
    // }

    // auto& internals()
    // {
    //     return entry.internals;
    // }

    // const HeaderPageHeader& headerPageHeader() const
    // {
    //     return header.headerPageHeader;
    // }

    // const NodePageHeader& nodePageHeader() const
    // {
    //     return header.nodePageHeader;
    // }

    // const FreePageHeader& freePageHeader() const
    // {
    //     return header.freePageHeader;
    // }

    // const auto& records() const
    // {
    //     return entry.records;
    // }

    // const auto& internals() const
    // {
    //     return entry.internals;
    // }

    // template <typename T>
    // void push_back(const T& value)
    // {
    //     static_assert(std::is_same<Record, T>::value ||
    //                   std::is_same<Internal, T>::value);
    //     using S = typename std::conditional<std::is_same<Record, T>::value,
    //                                         Records, Internals>::type;
    //     getEntry<S>()[static_cast<int>(
    //         getHeader<NodePageHeader>().numberOfKeys++)] = value;
    // }

    // template <typename T, typename K, typename B>
    // void emplace_back(const K& key, const B& v)
    // {
    //     static_assert(std::is_same<Record, T>::value ||
    //                   std::is_same<Internal, T>::value);

    //     if constexpr (std::is_same<Record, T>::value)
    //     {
    //         auto& entry = getEntry<Records>()[static_cast<int>(
    //             getHeader<NodePageHeader>().numberOfKeys++)];
    //         entry.key = key;
    //         entry.value = v;
    //     }
    //     else
    //     {
    //         auto& entry = getEntry<Internals>()[static_cast<int>(
    //             getHeader<NodePageHeader>().numberOfKeys++)];
    //         entry.key = key;
    //         entry.node_id = v;
    //     }
    // }

    // template <typename T>
    // void insert(const T& entry, int insertion_point)
    // {
    //     static_assert(std::is_same<Record, T>::value ||
    //                   std::is_same<Internal, T>::value);
    //     auto& header = nodePageHeader();
    //     using S = typename std::conditional<std::is_same<Record, T>::value,
    //                                         Records, Internals>::type;

    //     S& entries = getEntry<S>();

    //     for (int i = header.numberOfKeys; i > insertion_point; --i)
    //     {
    //         entries[i] = entries[i - 1];
    //     }

    //     ++header.numberOfKeys;
    //     entries[insertion_point] = entry;
    // }

    // template <typename T>
    // void erase(int erase_point)
    // {
    //     static_assert(std::is_same<Record, T>::value ||
    //                   std::is_same<Internal, T>::value);
    //     auto& header = nodePageHeader();
    //     using S = typename std::conditional<std::is_same<Record, T>::value,
    //                                         Records, Internals>::type;

    //     S& entries = getEntry<S>();

    //     for (int i = erase_point + 1; i < static_cast<int>(header.numberOfKeys);
    //          ++i)
    //     {
    //         entries[i - 1] = entries[i];
    //     }

    //     --header.numberOfKeys;
    // }

    // template <typename T>
    // decltype(auto) range(std::size_t begin = 0, std::size_t end = 0)
    // {
    //     static_assert(std::is_same<Record, T>::value ||
    //                   std::is_same<Internal, T>::value);
    //     if (end == 0)
    //     {
    //         end = getHeader<NodePageHeader>().numberOfKeys;
    //     }

    //     using S = typename std::conditional<std::is_same<Record, T>::value,
    //                                         Records, Internals>::type;
    //     return range_t(std::next(std::begin(getEntry<S>()), begin),
    //                    std::next(std::begin(getEntry<S>()), end));
    // }

    // template <typename T, typename It>
    // void range_copy(It it, int begin = 0, int end = -1)
    // {
    //     static_assert(std::is_same<Record, T>::value ||
    //                   std::is_same<Internal, T>::value);
    //     if (end == -1)
    //     {
    //         end = getHeader<NodePageHeader>().numberOfKeys;
    //     }
    //     using S = typename std::conditional<std::is_same<Record, T>::value,
    //                                         Records, Internals>::type;
    //     std::copy(std::next(std::begin(getEntry<S>()), begin),
    //               std::next(std::begin(getEntry<S>()), end), it);
    // }

    // template <typename T, typename It>
    // void range_assignment(It begin, It end)
    // {
    //     static_assert(std::is_same<Record, T>::value ||
    //                   std::is_same<Internal, T>::value);
    //     using S = typename std::conditional<std::is_same<Record, T>::value,
    //                                         Records, Internals>::type;
    //     std::copy(begin, end, std::begin(getEntry<S>()));
    //     getHeader<NodePageHeader>().numberOfKeys = std::distance(begin, end);
    // }

    // template <typename T>
    // int satisfy_condition_first(std::function<bool(const T&)> condition) const
    // {
    //     static_assert(std::is_same<Record, T>::value ||
    //                   std::is_same<Internal, T>::value);
    //     using S = typename std::conditional<std::is_same<Record, T>::value,
    //                                         Records, Internals>::type;
    //     int index{};
    //     auto n = static_cast<int>(getHeader<NodePageHeader>().numberOfKeys);
    //     auto& arr = getEntry<S>();
    //     while (index < n && !condition(arr[index]))
    //     {
    //         ++index;
    //     }
    //     return index;
    // }

    // template <typename T>
    // int index_key(keyType key) const
    // {
    //     static_assert(std::is_same<Record, T>::value ||
    //                   std::is_same<Internal, T>::value);
    //     using S = typename std::conditional<std::is_same<Record, T>::value,
    //                                         Records, Internals>::type;
    //     int index{};
    //     auto n = static_cast<int>(getHeader<NodePageHeader>().numberOfKeys);
    //     auto& arr = getEntry<S>();
    //     while (index < n && arr[index].key != key)
    //     {
    //         ++index;
    //     }
    //     return index == n ? -1 : index;
    // }

    // int key_grt(keyType key) const
    // {
    //     int index{};
    //     auto n = number_of_keys();
    //     auto& arr = internals();
    //     while (index < n && arr[index].key <= key)
    //     {
    //         ++index;
    //     }
    //     return index;
    // }

    // template <typename T>
    // T& get(std::size_t idx)
    // {
    //     static_assert(std::is_same<Record, T>::value ||
    //                   std::is_same<Internal, T>::value);
    //     using S = typename std::conditional<std::is_same<Record, T>::value,
    //                                         Records, Internals>::type;
    //     return getEntry<S>()[idx];
    // }

    // template <typename T>
    // T& first()
    // {
    //     return get<T>(0);
    // }

    // template <typename T>
    // T& back()
    // {
    //     return get<T>(number_of_keys() - 1);
    // }

    // void print_node();

    // int number_of_keys() const;
    // pagenum_t parent() const;
    // void set_parent(pagenum_t parent);
    // bool is_leaf() const;

    // pagenum_t leftmost() const;
    // void set_leftmost(pagenum_t leftmost);
    // pagenum_t next_leaf() const;
    // void set_next_leaf(pagenum_t next_leaf);
};

#endif /* __FRAME_HPP__*/