#include "file.h"

#include <memory.h>

void InitHeaderPage(struct page_t* page,
                    const struct HeaderPageHeader* headerPageHeader)
{
    memset(page, 0, sizeof(struct page_t));
    memcpy(&page->header.headerPageHeader, headerPageHeader,
           sizeof(struct HeaderPageHeader));
}