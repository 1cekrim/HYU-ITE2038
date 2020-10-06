#include "file_manager.h"

#include <stdlib.h>

#include "file.h"

// FileManager struct를 생성해 반환
struct FileManager* DmCreate()
{
    // TODO: 구현
}

// FileManager를 초기화. 성공하면 true 반환
bool FmInit(struct FileManager* fm)
{
    // TODO: 구현
}

// name 명으로 File을 열어 FileManager에 저장
bool FmOpen(struct FileManager* fm, const char* name)
{
    // TODO: 구현
}

// payload 포인터가 가리키는 공간부터 size만큼 읽어와 File의 seek 위치에 쓰기
bool FmWrite(struct FileManager* fm, long int seek, const void* payload,
             size_t size)
{
    // TODO: 구현
}

// File의 seek 위치부터 size만클 읽어와 target에 집어넣음
bool FmRead(struct FileManager* fm, long int seek, void* target, size_t size)
{
    // TODO: 구현
}

// File 닫기
bool FmClose(struct FileManager* fm)
{
    // TODO: 구현
}

// File에 Page 추가
pagenum_t FmPageCreate(struct FileManager* fm)
{
    // TODO: 구현
}

// Page 씀. 성공하면 true 반환
bool FmPageWrite(struct FileManager* fm, pagenum_t pagenum, struct page_t* page)
{
    // TODO: 구현
}

// Page 읽어옴. 성공하면 true 반환
bool FmPageRead(struct FileManager* fm, pagenum_t pagenum, struct page_t* page)
{
    // TODO: 구현
}

// pagenum에 해당하는 Page free. 성공하면 true 반환.
bool FmPageFree(struct FileManager* fm, pagenum_t pagenum)
{
    // TODO: 구현
}