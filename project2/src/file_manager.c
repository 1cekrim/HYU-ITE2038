#include "disk.h"
#include "file.h"

#include <stdlib.h>

struct DiskManager* DmCreate(struct DiskManager* dm)
{
    
    struct HeaderPageHeader* header = &dm->fileHeader;
}