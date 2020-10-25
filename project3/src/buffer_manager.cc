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
    CHECK_WITH_LOG(fileManager, false, "buffer manager open failure: %s",
                   name.c_str());
    return true;
}

bool BufferManager::commit(pagenum_t pagenum, const frame_t& frame)
{
    return BufferController::instance().put(manager_id, pagenum, frame);
}

bool BufferManager::load(pagenum_t pagenum, frame_t& frame)
{
    return BufferController::instance().get(manager_id, pagenum, frame);
}

pagenum_t BufferManager::create()
{
    return BufferController::instance().frame_id_to_pagenum(
        BufferController::instance().create(manager_id));
}

bool BufferManager::free(pagenum_t pagenum)
{
    return BufferController::instance().free(manager_id, pagenum);
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

bool BufferController::get(int file_id, pagenum_t pagenum, frame_t& frame)
{
    int index = find(file_id, pagenum);
    if (index == INVALID_BUFFER_INDEX)
    {
        index = load(file_id, pagenum);
    }
    std::cout << "get start. pagenum: " << pagenum << ", mru: " << mru
              << ", lru: " << lru << '\n';
    CHECK_WITH_LOG(index != INVALID_BUFFER_INDEX, false,
                   "Buffer load failure. file: %d / pagenum: %ld", file_id,
                   pagenum);
    frame = (*buffer)[index];
    frame.file_id = file_id;
    frame.retain();
    // std::cout << "index: " << index << ", pagenum: " << pagenum << "file_id:
    // " << file_id <<"mru: " << mru << ", lru: " << lru << ", prev: " <<
    // frame.prev << ", next: " << frame.next << '\n';
    CHECK(unlink_frame(index, frame));
    CHECK(update_recently_used(index, frame));
    // std::cout << "after index: " << index << ", pagenum: " << pagenum <<
    // "file_id: " << file_id <<"mru: " << mru << ", lru: " << lru << ", prev: "
    // << frame.prev << ", next: " << frame.next << '\n';
    return true;
}

bool BufferController::put(int file_id, pagenum_t pagenum, const frame_t& frame)
{
    int index = find(file_id, pagenum);
    std::cout << "put start. pagenum: " << pagenum << ", mru: " << mru
              << ", lru: " << lru << '\n';
    if (index == INVALID_BUFFER_INDEX)
    {
        index = load(file_id, pagenum);
    }
    CHECK_WITH_LOG(index != INVALID_BUFFER_INDEX, false,
                   "Buffer load failure. file: %d / pagenum: %ld", file_id,
                   pagenum);
    auto& buffer_frame = (*buffer)[index];

    // 지금은 병렬 x
    CHECK_WITH_LOG(buffer_frame.pin != 1, false, "concurrency not supported");

    buffer_frame = frame;
    buffer_frame.is_dirty = true;
    buffer_frame.release();

    // std::cout << "index: " << index << ", pagenum: " << pagenum << "file_id:
    // " << file_id <<"mru: " << mru << ", lru: " << lru << ", prev: " <<
    // buffer_frame.prev << ", next: " << buffer_frame.next << '\n';

    CHECK(unlink_frame(index, buffer_frame));
    CHECK(update_recently_used(index, buffer_frame));

    // std::cout << "after index: " << index << ", pagenum: " << pagenum <<
    // "file_id: " << file_id <<"mru: " << mru << ", lru: " << lru << ", prev: "
    // << buffer_frame.prev << ", next: " << buffer_frame.next << '\n';
    return true;
}

int BufferController::create(int file_id)
{
    std::cout << "create call\n";
    auto& fileManager = getFileManager(file_id);
    auto pagenum = fileManager.create();
    CHECK_WITH_LOG(pagenum != EMPTY_PAGE_NUMBER, INVALID_BUFFER_INDEX,
                   "create page failure: %d", file_id);

    int result = load(file_id, pagenum);

    std::cout << "pagenum: " << pagenum << ", result: " << result << '\n';
    CHECK_RET(result != INVALID_BUFFER_INDEX, INVALID_BUFFER_INDEX);
    CHECK_RET(update_recently_used(result, buffer->at(result)),
              INVALID_BUFFER_INDEX);
    return result;
}

pagenum_t BufferController::frame_id_to_pagenum(int frame_id) const
{
    return buffer->at(frame_id).pagenum;
}

bool BufferController::free(int file_id, pagenum_t pagenum)
{
    int index = find(file_id, pagenum);
    if (index != INVALID_BUFFER_INDEX)
    {
        CHECK_WITH_LOG(frame_free(index), false,
                       "frame free failure\nfile_id: %d / pagenu,:%ld", file_id,
                       pagenum);
    }
    return getFileManager(file_id).free(pagenum);
}

std::size_t BufferController::size() const
{
    return num_buffer;
}

std::size_t BufferController::capacity() const
{
    return buffer->size();
}

bool BufferController::update_recently_used(int buffer_index, frame_t& frame)
{
    std::cout << "update: mru:" << mru << ", lru: " << lru
              << ", prev: " << frame.prev << ", next: " << frame.next << '\n';
    frame.prev = mru;
    frame.next = INVALID_BUFFER_INDEX;

    if (lru == INVALID_BUFFER_INDEX)
    {
        lru = buffer_index;
    }

    if (mru != INVALID_BUFFER_INDEX)
    {
        (*buffer)[mru].next = buffer_index;
    }
    mru = buffer_index;

    return true;
}

bool BufferController::unlink_frame(int buffer_index, frame_t& frame)
{
    // std::cout << "unlink frame: prev: " << frame.prev << ", next: " <<
    // frame.next;
    if (frame.next != INVALID_BUFFER_INDEX)
    {
        // auto& target = buffer->at(frame.next);
        // CHECK_WITH_LOG(target.prev == buffer_index, false, "logical error
        // detected");
        buffer->at(frame.next).prev = frame.prev;
    }
    else
    {
        CHECK_WITH_LOG(mru == buffer_index, false,
                       "logical error detected: mru: %d, buffer_index: %d", mru,
                       buffer_index);
        mru = frame.prev;
    }
    if (frame.prev != INVALID_BUFFER_INDEX)
    {
        // auto& target = buffer->at(frame.prev);
        // CHECK_WITH_LOG(target.next == buffer_index, false, "logical error
        // detected");
        buffer->at(frame.prev).next = frame.next;
    }
    else
    {
        CHECK_WITH_LOG(lru == buffer_index, false,
                       "logical error detected: lru: %d, buffer_index: %d", lru,
                       buffer_index);
        lru = frame.next;
    }

    if (lru == buffer_index)
    {
    }

    return true;
}

int BufferController::find(int file_id, pagenum_t pagenum)
{
    for (std::size_t i = 0; i < capacity(); ++i)
    {
        auto& frame = buffer->at(i);
        if (frame.pagenum == pagenum && frame.file_id == file_id)
        {
            return i;
        }
    }
    return INVALID_BUFFER_INDEX;
}

int BufferController::frame_alloc()
{
    if (size() < capacity())
    {
        for (std::size_t i = 0; i < capacity(); ++i)
        {
            if (!(*buffer)[i].valid())
            {
                return i;
            }
        }
        CHECK_WITH_LOG(false, INVALID_BUFFER_INDEX,
                       "BUFFER CORRUPTION DETECTED");
    }

    int index = select_victim<BufferCircularLinearTraversalPolicy>();
    CHECK_WITH_LOG(index != INVALID_BUFFER_INDEX, INVALID_BUFFER_INDEX,
                   "alloc frame failure");

    auto& frame = buffer->at(index);
    frame.init();

    return index;
}

bool BufferController::frame_free(int buffer_index)
{
    auto& frame = buffer->at(buffer_index);

    // 지금은 병렬 x
    CHECK_WITH_LOG(!frame.is_use_now(), false, "concurrency not supported");

    while (frame.is_use_now())
    {
        // wait
    }

    if (frame.is_dirty)
    {
        CHECK(commit(frame.file_id, frame));
    }

    frame.init();

    return true;
}

int BufferController::load(int file_id, pagenum_t pagenum)
{
    auto& fileManager = getFileManager(file_id);
    int index = frame_alloc();
    std::cout << "index: " << index << '\n';

    CHECK_RET(index != INVALID_BUFFER_INDEX, INVALID_BUFFER_INDEX);
    auto& frame = buffer->at(index);

    frame.init();
    CHECK_WITH_LOG(fileManager.load(pagenum, frame), INVALID_BUFFER_INDEX,
                   "frame load failure: %ld", pagenum);
    frame.file_id = file_id;
    frame.pagenum = pagenum;
    ++num_buffer;

    CHECK_WITH_LOG(update_recently_used(index, frame), INVALID_BUFFER_INDEX,
                   "update recently used failure");

    return index;
}

bool BufferController::commit(int file_id, const frame_t& frame)
{
    auto& fileManager = getFileManager(file_id);
    CHECK_WITH_LOG(fileManager.commit(frame.pagenum, frame), false,
                   "commit frame failure: %ld", frame.pagenum);
    return false;
}