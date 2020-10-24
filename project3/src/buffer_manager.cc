#include "buffer_manager.hpp"
#include "frame.hpp"

BufferManager::BufferManager() : fileManager()
{
    // TODO: 구현
    // Do nothing
}

BufferManager::~BufferManager()
{
    // TODO: 구현
    // Do nothing
}

bool BufferManager::open(const std::string& name)
{
    // TODO: 구현
    return fileManager.open(name);
}

bool BufferManager::commit(pagenum_t pagenum, const frame_t& frame)
{
    // TODO: 구현
    return fileManager.commit(pagenum, frame);
}

bool BufferManager::load(pagenum_t pagenum, frame_t& frame)
{
    return fileManager.load(pagenum, frame);
}

pagenum_t BufferManager::create()
{
    // TODO: 구현
    return fileManager.create();
}

bool BufferManager::free(pagenum_t pagenum)
{
    // TODO: 구현
    return fileManager.free(pagenum);
}

int BufferManager::get_manager_id() const
{
    return fileManager.get_manager_id();
}

pagenum_t BufferManager::root() const
{
    return fileManager.root();
}

bool BufferManager::set_root(pagenum_t pagenum)
{
    // TODO: 구현
    return fileManager.set_root(pagenum);
}

const HeaderPageHeader& BufferManager::getFileHeader() const
{
    // TODO: 구현
    return fileManager.getFileHeader();
}
