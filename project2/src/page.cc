#include "page.hpp"

#include <iostream>

std::ostream& operator<<(std::ostream& os, const NodePageHeader& nph)
{
    os << "parentPageNumber: " << nph.parentPageNumber
       << "\nisLeaf: " << nph.isLeaf << "\nnumberOfKeys: " << nph.numberOfKeys
       << "\nonePageNumber: " << nph.onePageNumber << "\n";
    return os;
}

std::ostream& operator<<(std::ostream& os, const HeaderPageHeader& hph)
{
    os << "freePageNumber:" << hph.freePageNumber
       << "\nnumberOfPages:" << hph.numberOfPages
       << "\nrootPageNumber: " << hph.rootPageNumber << '\n';
    return os;
}