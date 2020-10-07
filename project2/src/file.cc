#include "file.hpp"

#include <memory.h>

void InitHeaderPage(page_t& page, const HeaderPageHeader& headerPageHeader)
{
    memset(&page, 0, sizeof(page_t));
    memcpy(&page.header.headerPageHeader, &headerPageHeader,
           sizeof(HeaderPageHeader));
}