#ifndef __FILE_MANAGER_HPP__
#define __FILE_MANAGER_HPP__

#include <unistd.h>

#include <cstdio>
#include <memory>
#include <string_view>
#include <vector>

#include "frame.hpp"
#include "page.hpp"

class BufferManager;

constexpr auto FILE_HEADER_PAGENUM = 0;

struct header_frame
{
   frame_t frame;
   int buffer_index = -1;
   ~header_frame();
};

class FileManager
{
 public:
    FileManager();
    ~FileManager();

    // node manager interface
    bool open(const std::string& name);
    bool commit(pagenum_t pagenum, const page_t& page);
    bool load(pagenum_t pagenum, page_t& page);
    pagenum_t create();
    bool free(pagenum_t pagenum);
    int get_manager_id() const;
    pagenum_t root() const;
    bool set_root(pagenum_t pagenum);
    void set_buffer_manager(BufferManager* bufferManager);

    bool init_file_if_created();

 private:
    int fd;
    BufferManager* bufferManager;
    bool file_created;
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

    bool get_file_header(header_frame& header) const;
    bool set_file_header(const header_frame& header);

    // 새 페이지 추가
    pagenum_t pageCreate();
};

#endif /* __FILE_MANAGER_HPP__*/