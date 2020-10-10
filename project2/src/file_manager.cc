#include "file_manager.hpp"

#include <fcntl.h>

#include <cstdlib>
#include <cstring>
#include <iostream>

#include "logger.hpp"

FileManager::FileManager() : fp(nullptr, &std::fclose)
{
    fileHeader.Init();
}

FileManager::~FileManager()
{
    if (fp)
    {
        updateFileHeader();
        fp.reset();
    }
}

bool FileManager::open(const std::string& name)
{
    // 파일 없음
    if (access(name.c_str(), F_OK) == -1)
    {
        std::unique_ptr<std::FILE, decltype(&std::fclose)> p(
            fdopen(::open(name.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DSYNC | O_DIRECT,
                          0755),
                   "r+"),
            &std::fclose);
        fp = std::move(p);
        ASSERT_WITH_LOG(fp, false, "create file failure");

        page_t headerPage;
        headerPage.header.headerPageHeader.numberOfPages = 1;
        fileHeader.numberOfPages = 1;
        ASSERT_WITH_LOG(pageWrite(0, headerPage), false,
                        "write header page failure: 0");
    }
    else
    {
        std::unique_ptr<std::FILE, decltype(&std::fclose)> p(
            fdopen(::open(name.c_str(), O_RDWR | O_DSYNC | O_DIRECT), "r+"), &std::fclose);
        fp = std::move(p);
        ASSERT_WITH_LOG(fp, false, "open file failure");

        page_t headerPage;
        ASSERT_WITH_LOG(pageRead(0, headerPage), false,
                        "read header page failure: 0");

        fileHeader = headerPage.header.headerPageHeader;
    }

    return true;
}

bool FileManager::write(long int seek, const void* payload, std::size_t size)
{
    if (!fp)
    {
        std::cout << "fuck\n";
    }
    long count = pwrite(fileno(fp.get()), payload, size, seek);
    std::fflush(fp.get());

    return count != -1;
}

bool FileManager::read(long int seek, void* target, size_t size)
{
    if (!fp)
    {
        std::cout << seek << '\n' << size << "fuck\n";
    }
    long count = pread(fileno(fp.get()), target, size, seek);

    return count != -1;
}

// File에 Page 추가
pagenum_t FileManager::pageCreate()
{
    auto pagenum = fileHeader.freePageNumber;
    if (pagenum == EMPTY_PAGE_NUMBER)
    {
        pagenum = fileHeader.numberOfPages;
        page_t page;
        ASSERT_WITH_LOG(pageWrite(pagenum, page), EMPTY_PAGE_NUMBER,
                        "write page failure: %ld", pagenum);
        ASSERT_WITH_LOG(pageRead(pagenum, page), EMPTY_PAGE_NUMBER,
                        "write page failure: %ld", pagenum);
        ++fileHeader.numberOfPages;
    }
    else
    {
        page_t page;
        ASSERT_WITH_LOG(pageRead(pagenum, page), EMPTY_PAGE_NUMBER,
                        "read page failure: %ld", pagenum);
        fileHeader.freePageNumber =
            page.header.freePageHeader.nextFreePageNumber;
    }

    ASSERT_WITH_LOG(updateFileHeader(), EMPTY_PAGE_NUMBER,
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
    page_t page;
    ASSERT_WITH_LOG(pageRead(pagenum, page), false, "page read failure: %ld",
                    pagenum);
    page.header.freePageHeader.nextFreePageNumber = fileHeader.freePageNumber;
    ASSERT_WITH_LOG(pageWrite(pagenum, page), false, "page write failure: %ld",
                    pagenum);
    fileHeader.freePageNumber = pagenum;
    ASSERT_WITH_LOG(updateFileHeader(), false, "update file header failure");
    return true;
}

bool FileManager::updateFileHeader()
{
    return write(0, &fileHeader, sizeof(HeaderPageHeader));
}

int FileManager::getFileDescriptor() const
{
    if (!fp)
    {
        return -1;
    }
    return fileno(fp.get());
}