#include "file_manager.hpp"

#include <fcntl.h>

#include <cstdlib>
#include <cstring>

FileManager::FileManager() : fp(nullptr, &std::fclose)
{
    fileHeader.Init();
}

bool FileManager::open(const std::string& name)
{
    // 파일 없음
    if (access(name.c_str(), F_OK) == -1)
    {
        std::unique_ptr<std::FILE, decltype(&std::fclose)> p(
            fdopen(::open(name.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DSYNC,
                          0755),
                   "r+"),
            &std::fclose);
        fp = std::move(p);
        if (!fp)
        {
            return false;
        }

        page_t headerPage;
        if (!pageWrite(0, headerPage))
        {
            return false;
        }
    }
    else
    {
        std::unique_ptr<std::FILE, decltype(&std::fclose)> p(
            fdopen(::open(name.c_str(), O_RDWR | O_DSYNC), "r+"), &std::fclose);
        fp = std::move(p);
        if (!fp)
        {
            return false;
        }

        page_t headerPage;
        if (!pageRead(0, headerPage))
        {
            return false;
        }

        fileHeader = headerPage.header.headerPageHeader;
    }

    return true;
}

bool FileManager::write(long int seek, const void* payload, std::size_t size)
{
    std::fseek(fp.get(), seek, SEEK_SET);

    std::size_t count = std::fwrite(payload, size, 1, fp.get());
    std::fflush(fp.get());

    return count == 1;
}

bool FileManager::read(long int seek, void* target, size_t size)
{
    std::fseek(fp.get(), seek, SEEK_SET);

    std::size_t count = std::fread(target, size, 1, fp.get());

    return count == 1;
}

bool FileManager::close()
{
    // TODO: 구현
    return false;
}

// File에 Page 추가
pagenum_t FileManager::pageCreate()
{
    // TODO: 구현
    return false;
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
    // TODO: 구현
    return false;
}

bool FileManager::updateFileHeader()
{
    return write(0, &fileHeader, sizeof(HeaderPageHeader));
}