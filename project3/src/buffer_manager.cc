#include "buffer_manager.hpp"
#include "frame.hpp"
#include "logger.hpp"

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
    auto& bc = BufferController::instance();
    manager_id = bc.openFileManager(name);
    fileManager = &bc.getFileManager(manager_id);
    CHECK_WITH_LOG(fileManager, false, "buffer manager open failure: %s", name.c_str());
    return true;
}

bool BufferManager::commit(pagenum_t pagenum, const frame_t& frame)
{
    // TODO: 구현
    return fileManager->commit(pagenum, frame);
}

bool BufferManager::load(pagenum_t pagenum, frame_t& frame)
{
    return fileManager->load(pagenum, frame);
}

pagenum_t BufferManager::create()
{
    // TODO: 구현
    return fileManager->create();
}

bool BufferManager::free(pagenum_t pagenum)
{
    // TODO: 구현
    return fileManager->free(pagenum);
}

int BufferManager::get_manager_id() const
{
    return manager_id;
}

pagenum_t BufferManager::root() const
{
    return fileManager->root();
}

bool BufferManager::set_root(pagenum_t pagenum)
{
    // TODO: 구현
    return fileManager->set_root(pagenum);
}

const HeaderPageHeader& BufferManager::getFileHeader() const
{
    // TODO: 구현
    return fileManager->getFileHeader();
}

int BufferController::openFileManager(const std::string& name)
{
    if (nameFileManagerMap.find(name) == nameFileManagerMap.end())
    {
        int id = fileManagers.size();
        auto& fm = fileManagers.emplace_back(new FileManager());
        CHECK_RET(fm->open(name), -1);
        nameFileManagerMap.emplace(name, id);
        return id;
    }

    return nameFileManagerMap[name];
}

FileManager& BufferController::getFileManager(int file_id)
{
    return *fileManagers.at(file_id);
}