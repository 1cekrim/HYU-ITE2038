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
    auto& head = header.nodePageHeader;
    std::cout << head;
    if (head.isLeaf)
    {
        auto& body = entry.records;
        for (int i = 0; i < static_cast<int>(head.numberOfKeys); ++i)
        {
            std::cout << "[" << i << "] (" << body[i].key << ", " << body[i].value
                      << ")\n";
        }
    }
    else
    {
        auto& body = entry.internals;
        for (int i = 0; i < static_cast<int>(head.numberOfKeys); ++i)
        {
            std::cout << "[" << i << "] (" << body[i].key << ", " << body[i].pageNumber
                      << ")\n";
        }
    }
}