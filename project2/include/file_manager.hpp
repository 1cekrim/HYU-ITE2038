#ifndef __DISK_H__
#define __DISK_H__

#include <stdbool.h>
#include <stdio.h>
#include <unistd.h>

#include "file.hpp"

struct FileManager
{
    FILE* fp;
    struct HeaderPageHeader fileHeader;
};

// FileManager struct를 생성해 반환
struct FileManager* FmCreate();

// FileManager를 초기화. 성공하면 true 반환
bool FmInit(struct FileManager* fm);

// name 명으로 File을 열어 FileManager에 저장
bool FmOpen(struct FileManager* fm, const char* name);

// payload 포인터가 가리키는 공간부터 size만큼 읽어와 File의 seek 위치에 쓰기
bool FmWrite(struct FileManager* fm, long int seek, const void* payload,
             size_t size);

// File의 seek 위치부터 size만클 읽어와 target에 집어넣음
bool FmRead(struct FileManager* fm, long int seek, void* target, size_t size);

// File 닫기
bool FmClose(struct FileManager* fm);

// File에 Page 추가
pagenum_t FmPageCreate(struct FileManager* fm);

// Page 씀. 성공하면 true 반환
bool FmPageWrite(struct FileManager* fm, pagenum_t pagenum,
                 struct page_t* page);

// Page 읽어옴. 성공하면 true 반환
bool FmPageRead(struct FileManager* fm, pagenum_t pagenum, struct page_t* page);

// pagenum에 해당하는 Page free. 성공하면 true 반환.
bool FmPageFree(struct FileManager* fm, pagenum_t pagenum);

#endif /* __DISK_H__*/