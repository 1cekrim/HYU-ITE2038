#include "page.hpp"

#include <iostream>

std::ostream& operator<<(std::ostream& os, const NodePageHeader& nph)
{
    os << "parentPageNumber: " << nph.parentPageNumber
       << "\nisLeaf: " << nph.isLeaf << "\nnumberOfKeys: " << nph.numberOfKeys
       << "\nonePageNumber: " << nph.onePageNumber << "\n";
}