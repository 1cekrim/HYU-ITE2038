#ifndef __FILE_MANAGER_HPP__
#define __FILE_MANAGER_HPP__

#include <unistd.h>

#include <cstdio>
#include <memory>
#include <string_view>

#include "page.hpp"

class FileManager
{
 public:
    FileManager();
    ~FileManager() = default;

    // name 명으로 File 열기
    bool open(const std::string& name);

    // 파일 닫기
    bool close();

    // payload 포인터가 가리키는 공간부터 size만큼 읽어와 File의 seek 위치에
    // 쓰기
    bool write(long int seek, const void* payload, std::size_t size);

    // File의 seek 위치부터 size만클 읽어와 target에 집어넣음
    bool read(long int seek, void* target, std::size_t size);

    // Page 씀. 성공하면 true 반환
    bool pageWrite(pagenum_t pagenum, const page_t& page);

    // Page 읽어옴. 성공하면 true 반환
    bool pageRead(pagenum_t pagenum, page_t& page);

    // pagenum에 해당하는 Page free. 성공하면 true 반환.
    bool pageFree(pagenum_t pagenum);

    // 새 페이지 추가
    pagenum_t pageCreate();

    const HeaderPageHeader& getHeaderPageHeader() const;

 private:
    std::unique_ptr<std::FILE, decltype(&std::fclose)> fp;
    HeaderPageHeader fileHeader;
};

#endif /* __FILE_MANAGER_HPP__*/