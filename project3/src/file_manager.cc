#include "file_manager.hpp"

#include <fcntl.h>

#include <cstdlib>
#include <cstring>
#include <iostream>

#include "buffer_manager.hpp"
#include "logger.hpp"

FileManager::FileManager() : fd(-1), file_created(false)
{
    // Do nothing
}

FileManager::~FileManager()
{
    // Do nothing
}

bool FileManager::open(const std::string& name)
{
    // 파일 없음
    if (access(name.c_str(), F_OK) == -1)
    {
        fd = ::open(name.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
        CHECK_WITH_LOG(fd != -1, false, "create file failure");

        file_created = true;
    }
    else
    {
        fd = ::open(name.c_str(), O_RDWR);
        CHECK_WITH_LOG(fd != -1, false, "open file failure");
    }

    return true;
}
bool FileManager::init_file_if_created()
{
    if (!file_created)
    {
        return true;
    }
    file_created = false;
    header_frame headerPage {};
    headerPage.frame.headerPageHeader().numberOfPages = 1;
    CHECK(set_file_header(headerPage));
    return true;
}

bool FileManager::get_file_header(header_frame& header) const
{
    int index = bufferManager->load(FILE_HEADER_PAGENUM, header.frame);
    CHECK_WITH_LOG(index != INVALID_BUFFER_INDEX, false,
                   "get file header failure");
    header.buffer_index = index;
    return true;
}

bool FileManager::set_file_header(const header_frame& header)
{
    CHECK_WITH_LOG(bufferManager->commit(FILE_HEADER_PAGENUM, header.frame), false,
                   "set file header failure");
    return true;
}

void FileManager::set_buffer_manager(BufferManager* bufferManager)
{
    this->bufferManager = bufferManager;
}

bool FileManager::write(long int seek, const void* payload, std::size_t size)
{
    long count = pwrite(fd, payload, size, seek);
    int result = fsync(fd);

    return !result && count != -1;
}

bool FileManager::read(long int seek, void* target, size_t size)
{
    long count = pread(fd, target, size, seek);

    return count != -1;
}

header_frame::~header_frame()
{
    if (buffer_index != -1)
    {
        BufferController::instance().release_frame(buffer_index);
    }
}

// File에 Page 추가
pagenum_t FileManager::pageCreate()
{
    header_frame header;
    CHECK_RET(get_file_header(header), EMPTY_PAGE_NUMBER);
    auto& fileHeader = header.frame.headerPageHeader();
    auto pagenum = fileHeader.freePageNumber;
    if (pagenum == EMPTY_PAGE_NUMBER)
    {
        pagenum = fileHeader.numberOfPages;
        page_t page {};
        CHECK_WITH_LOG(pageWrite(pagenum, page), EMPTY_PAGE_NUMBER,
                       "write page failure: %ld", pagenum);
        CHECK_WITH_LOG(pageRead(pagenum, page), EMPTY_PAGE_NUMBER,
                       "write page failure: %ld", pagenum);
        ++fileHeader.numberOfPages;
    }
    else
    {
        page_t page {};
        CHECK_WITH_LOG(pageRead(pagenum, page), EMPTY_PAGE_NUMBER,
                       "read page failure: %ld", pagenum);
        fileHeader.freePageNumber = page.freePageHeader().nextFreePageNumber;
    }

    CHECK_WITH_LOG(set_file_header(header), EMPTY_PAGE_NUMBER,
                   "update file header failure");
    return pagenum;
}

// Page 씀. 성공하면 true 반환
bool FileManager::pageWrite(pagenum_t pagenum, const page_t& page)
{
    return write(PAGESIZE * pagenum, &page, sizeof(page_t));
}

// Page 읽어옴. 성공하면 true 반환
bool FileManager::pageRead(pagenum_t pagenum, page_t& page)
{
    return read(PAGESIZE * pagenum, &page, sizeof(page_t));
}

// pagenum에 해당하는 Page free. 성공하면 true 반환.
bool FileManager::pageFree(pagenum_t pagenum)
{
    page_t page {};
    CHECK_WITH_LOG(pageRead(pagenum, page), false, "page read failure: %ld",
                   pagenum);

    header_frame header;
    CHECK(get_file_header(header));
    auto& fileHeader = header.frame.headerPageHeader();

    page.freePageHeader().nextFreePageNumber = fileHeader.freePageNumber;
    CHECK_WITH_LOG(pageWrite(pagenum, page), false, "page write failure: %ld",
                   pagenum);
    fileHeader.freePageNumber = pagenum;

    CHECK_WITH_LOG(set_file_header(header), EMPTY_PAGE_NUMBER,
                   "update file header failure");
    return true;
}

int FileManager::get_manager_id() const
{
    if (fd == -1)
    {
        return -1;
    }
    return fd;
}

pagenum_t FileManager::root() const
{
    header_frame header;
    CHECK_RET(get_file_header(header), EMPTY_PAGE_NUMBER);

    return header.frame.headerPageHeader().rootPageNumber;
}

bool FileManager::set_root(pagenum_t pagenum)
{
    header_frame header;
    CHECK_RET(get_file_header(header), EMPTY_PAGE_NUMBER);
    auto& fileHeader = header.frame.headerPageHeader();

    fileHeader.rootPageNumber = pagenum;
    CHECK_WITH_LOG(set_file_header(header), EMPTY_PAGE_NUMBER,
                   "update file header failure");

    return true;
}

bool FileManager::commit(pagenum_t pagenum, const page_t& page)
{
    CHECK_WITH_LOG(pageWrite(pagenum, page), false, "write page failure: %ld",
                   pagenum);
    return true;
}

bool FileManager::load(pagenum_t pagenum, page_t& page)
{
    CHECK_WITH_LOG(pageRead(pagenum, page), false, "read page failure: %ld",
                   pagenum);
    return true;
}

pagenum_t FileManager::create()
{
    return pageCreate();
}

bool FileManager::free(pagenum_t pagenum)
{
    return pageFree(pagenum);
}