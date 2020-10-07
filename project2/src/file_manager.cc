#include "file_manager.hpp"

#include <fcntl.h>
#include <memory.h>
#include <stdlib.h>

#include "file.hpp"

// FileManager struct를 생성해 반환
struct FileManager* FmCreate()
{
    struct FileManager* fm =
        (struct FileManager*)malloc(sizeof(struct FileManager));
    return fm;
}

// FileManager를 초기화. 성공하면 true 반환
bool FmInit(struct FileManager* fm)
{
    if (!fm)
    {
        return false;
    }

    fm->fp = NULL;
    fm->fileHeader.freePageNumber = 0;
    fm->fileHeader.numberOfPages = 0;
    fm->fileHeader.rootPageNumber = 0;
    memset(fm->fileHeader.reserved, 0, sizeof(fm->fileHeader.reserved));

    return true;
}

// name 명으로 File을 열어 FileManager에 저장
bool FmOpen(struct FileManager* fm, const char* name)
{
    // 파일 없음
    if (access(name, F_OK) == -1)
    {
        if (!FmInit(fm))
        {
            // fm == NULL
            return false;
        }

        fm->fp = fdopen(open(name, O_RDWR | O_CREAT | O_TRUNC | O_DSYNC, 0755),
                        "r+");
        if (!fm->fp)
        {
            return false;
        }

        struct page_t headerPage;
        InitHeaderPage(&headerPage, &fm->fileHeader);
        if (!FmPageWrite(fm, 0, &headerPage))
        {
            return false;
        }
    }
    else
    {
        fm->fp = fdopen(open(name, O_RDWR | O_DSYNC), "r+");
        if (fm->fp == NULL)
        {
            return false;
        }

        struct page_t headerPage;
        if (!FmPageRead(fm, 0, &headerPage))
        {
            return false;
        }

        memcpy(&fm->fileHeader, &headerPage.header.headerPageHeader,
               sizeof(struct HeaderPageHeader));
    }

    return true;
}

// payload 포인터가 가리키는 공간부터 size만큼 읽어와 File의 seek 위치에 쓰기
bool FmWrite(struct FileManager* fm, long int seek, const void* payload,
             size_t size)
{
    FILE* fp = fm->fp;
    fseek(fp, seek, SEEK_SET);

    size_t count = fwrite(payload, size, 1, fp);
    fflush(fp);

    return count == 1;
}

// File의 seek 위치부터 size만클 읽어와 target에 집어넣음
bool FmRead(struct FileManager* fm, long int seek, void* target, size_t size)
{
    FILE* fp = fm->fp;
    fseek(fp, seek, SEEK_SET);

    size_t count = fread(target, size, 1, fp);

    return count == 1;
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
    return FmWrite(fm, PAGESIZE * pagenum, page, sizeof(struct page_t));
}

// Page 읽어옴. 성공하면 true 반환
bool FmPageRead(struct FileManager* fm, pagenum_t pagenum, struct page_t* page)
{
    return FmRead(fm, PAGESIZE * pagenum, page, sizeof(struct page_t));
}

// pagenum에 해당하는 Page free. 성공하면 true 반환.
bool FmPageFree(struct FileManager* fm, pagenum_t pagenum)
{
    // TODO: 구현
}