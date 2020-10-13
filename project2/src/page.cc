#include "page.hpp"

#include <iostream>

std::ostream& operator<<(std::ostream& os, const NodePageHeader& nph)
{
    os << "\nparentPageNumber: " << nph.parentPageNumber
       << "\nisLeaf: " << nph.isLeaf << "\nnumberOfKeys: " << nph.numberOfKeys
       << "\nonePageNumber: " << nph.onePageNumber << "\n";
    return os;
}

std::ostream& operator<<(std::ostream& os, const HeaderPageHeader& hph)
{
    os << "\nfreePageNumber:" << hph.freePageNumber
       << "\nnumberOfPages:" << hph.numberOfPages
       << "\nrootPageNumber: " << hph.rootPageNumber << '\n';
    return os;
}

void page_t::print_node()
{
    auto& head = nodePageHeader();
    std::cout << head;
    if (head.isLeaf)
    {
        auto& body = records();
        for (int i = 0; i < static_cast<int>(head.numberOfKeys); ++i)
        {
            std::cout << "[" << i << "] (" << body[i].key << ", "
                      << body[i].value << ")\n";
        }
    }
    else
    {
        auto& body = internals();
        for (int i = 0; i < static_cast<int>(head.numberOfKeys); ++i)
        {
            std::cout << "[" << i << "] (" << body[i].key << ", "
                      << body[i].pageNumber << ")\n";
        }
    }
}

template <>
const Records& page_t::getEntry() const
{
    return entry.records;
}

template <>
const Internals& page_t::getEntry() const
{
    return entry.internals;
}

template <>
const HeaderPageHeader& page_t::getHeader() const
{
    return header.headerPageHeader;
}

template <>
const NodePageHeader& page_t::getHeader() const
{
    return header.nodePageHeader;
}

template <>
const FreePageHeader& page_t::getHeader() const
{
    return header.freePageHeader;
}

template <>
Records& page_t::getEntry()
{
    return const_cast<Records&>(std::as_const(*this).getEntry<Records>());
}

template <>
Internals& page_t::getEntry()
{
    return const_cast<Internals&>(std::as_const(*this).getEntry<Internals>());
}

template <>
HeaderPageHeader& page_t::getHeader()
{
    return const_cast<HeaderPageHeader&>(
        std::as_const(*this).getHeader<HeaderPageHeader>());
}

template <>
NodePageHeader& page_t::getHeader()
{
    return const_cast<NodePageHeader&>(
        std::as_const(*this).getHeader<NodePageHeader>());
}

template <>
FreePageHeader& page_t::getHeader()
{
    return const_cast<FreePageHeader&>(
        std::as_const(*this).getHeader<FreePageHeader>());
}