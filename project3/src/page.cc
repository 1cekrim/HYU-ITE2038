#include "page.hpp"
#include <iostream>
#include "file_manager.hpp"
#include "logger.hpp"

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
                      << body[i].value.data() << ")\n";
        }
    }
    else
    {
        auto& body = internals();
        for (int i = 0; i < static_cast<int>(head.numberOfKeys); ++i)
        {
            std::cout << "[" << i << "] (" << body[i].key << ", "
                      << body[i].node_id << ")\n";
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

int page_t::number_of_keys() const
{
    return static_cast<int>(nodePageHeader().numberOfKeys);
}

pagenum_t page_t::parent() const
{
    return nodePageHeader().parentPageNumber;
}

void page_t::set_parent(pagenum_t parent)
{
    nodePageHeader().parentPageNumber = parent;
}

bool page_t::is_leaf() const
{
    return nodePageHeader().isLeaf;
}

void page_t::set_is_leaf(bool is_leaf)
{
    nodePageHeader().isLeaf = is_leaf;
}

pagenum_t page_t::leftmost() const
{
    // Check isleaf
    return nodePageHeader().onePageNumber;
}

void page_t::set_leftmost(pagenum_t leftmost)
{
    nodePageHeader().onePageNumber = leftmost;
}

pagenum_t page_t::next_leaf() const
{
    return nodePageHeader().onePageNumber;
}

void page_t::set_next_leaf(pagenum_t next_leaf)
{
    nodePageHeader().onePageNumber = next_leaf;
}