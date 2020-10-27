#include "buffer_manager.hpp"

#include "frame.hpp"
#include "logger.hpp"

BufferManager::BufferManager() : fileManager()
{
    // Do nothing
}

BufferManager::~BufferManager()
{
    close();
}

bool BufferManager::close()
{
    if (manager_id == -1 || !fileManager)
    {
        return false;
    }

    CHECK(BufferController::instance().fsync(manager_id, true));
    return true;
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

int BufferManager::load(pagenum_t pagenum, frame_t& frame)
{
    int result = BufferController::instance().get(manager_id, pagenum, frame);
    if (result != INVALID_BUFFER_INDEX)
    {
        BufferController::instance().retain_frame(result);
    }
    return result;
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

bool BufferController::init_buffer(std::size_t buffer_size)
{
    CHECK_WITH_LOG(this->buffer_size < buffer_size, false,
                   "cannot be made smaller than the current buffer size %ld",
                   this->buffer_size);
    this->buffer_size = buffer_size;

    buffer = std::make_unique<std::vector<frame_t>>(buffer_size);
    CHECK_WITH_LOG(buffer.get(), false, "alloc buffer failure");
    for (auto& frame : *buffer)
    {
        frame.init();
    }

    fileManagers.clear();
    index_table.clear();
    nameFileManagerMap.clear();

    mru = INVALID_BUFFER_INDEX;
    lru = INVALID_BUFFER_INDEX;
    num_buffer = 0;

    free_indexes = std::make_unique<std::stack<int>>();
    for (int i = buffer_size - 1; i >= 0; --i)
    {
        free_indexes->push(i);
    }
    return true;
}

bool BufferController::clear_buffer()
{
    CHECK_WITH_LOG(sync(), false, "sync failure");
    buffer.reset();
    fileManagers.clear();
    index_table.clear();
    nameFileManagerMap.clear();
    free_indexes.reset();
    mru = INVALID_BUFFER_INDEX;
    lru = INVALID_BUFFER_INDEX;
    num_buffer = 0;
    buffer_size = 0;
    return true;
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

void BufferController::release_frame(int frame_index)
{
    buffer->at(frame_index).release();
}

void BufferController::retain_frame(int frame_index)
{
    buffer->at(frame_index).retain();
}

int BufferController::get(int file_id, pagenum_t pagenum, frame_t& frame)
{
    int index = find(file_id, pagenum);
    if (index == INVALID_BUFFER_INDEX)
    {
        index = load(file_id, pagenum);
    }

    CHECK_WITH_LOG(index != INVALID_BUFFER_INDEX, INVALID_BUFFER_INDEX,
                   "Buffer load failure. file: %d / pagenum: %ld", file_id,
                   pagenum);
    auto& buffer_frame = (*buffer)[index];
    frame = buffer_frame;

    CHECK_RET(unlink_frame(index, buffer_frame), INVALID_BUFFER_INDEX);

    CHECK_RET(update_recently_used(index, buffer_frame), INVALID_BUFFER_INDEX);

    return index;
}

bool BufferController::put(int file_id, pagenum_t pagenum, const frame_t& frame)
{
    int index = find(file_id, pagenum);
    if (index == INVALID_BUFFER_INDEX)
    {
        index = load(file_id, pagenum);
    }
    CHECK_WITH_LOG(index != INVALID_BUFFER_INDEX, false,
                   "Buffer load failure. file: %d / pagenum: %ld", file_id,
                   pagenum);
    auto& buffer_frame = (*buffer)[index];

    // 지금은 병렬 x
    buffer_frame.change_page(frame);
    buffer_frame.is_dirty = true;

    CHECK(unlink_frame(index, buffer_frame));
    CHECK(update_recently_used(index, buffer_frame));

    return true;
}

int BufferController::create(int file_id)
{
    auto& fileManager = getFileManager(file_id);
    auto pagenum = fileManager.create();
    CHECK_WITH_LOG(pagenum != EMPTY_PAGE_NUMBER, INVALID_BUFFER_INDEX,
                   "create page failure: %d", file_id);

    int result = load(file_id, pagenum);

    CHECK_RET(result != INVALID_BUFFER_INDEX, INVALID_BUFFER_INDEX);
    CHECK_RET(unlink_frame(result, buffer->at(result)), INVALID_BUFFER_INDEX);
    CHECK_RET(update_recently_used(result, buffer->at(result)),
              INVALID_BUFFER_INDEX);

    return result;
}

pagenum_t BufferController::frame_id_to_pagenum(int frame_id) const
{
    CHECK_WITH_LOG(frame_id != INVALID_BUFFER_INDEX, EMPTY_PAGE_NUMBER,
                   "invalid frame id: %d", frame_id);
    pagenum_t result = buffer->at(frame_id).pagenum;
    return result;
}

bool BufferController::free(int file_id, pagenum_t pagenum)
{
    int index = find(file_id, pagenum);
    if (index != INVALID_BUFFER_INDEX)
    {
        CHECK_WITH_LOG(frame_free(index), false,
                       "frame free failure\nfile_id: %d / pagenu,:%ld", file_id,
                       pagenum);
        free_indexes->push(index);
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
    if (frame.prev != INVALID_BUFFER_INDEX)
    {
        buffer->at(frame.prev).next = frame.next;
    }
    else
    {
        CHECK_WITH_LOG(lru == buffer_index, false,
                       "logical error detected: lru: %d, buffer_index: %d", lru,
                       buffer_index);
        lru = frame.next;
    }

    if (frame.next != INVALID_BUFFER_INDEX)
    {
        buffer->at(frame.next).prev = frame.prev;
    }
    else
    {
        CHECK_WITH_LOG(mru == buffer_index, false,
                       "logical error detected: mru: %d, buffer_index: %d", mru,
                       buffer_index);
        mru = frame.prev;
    }

    return true;
}

int BufferController::find(int file_id, pagenum_t pagenum)
{
    if (index_table[file_id].find(pagenum) != index_table[file_id].end())
    {
        return index_table[file_id][pagenum];
    }
    return INVALID_BUFFER_INDEX;
}

void BufferController::memorize_index(int file_id, pagenum_t pagenum,
                                      int frame_index)
{
    index_table[file_id][pagenum] = frame_index;
}

void BufferController::forget_index(int file_id, pagenum_t pagenum)
{
    index_table[file_id].erase(pagenum);
}

bool BufferController::sync()
{
    for (auto& frame : *buffer)
    {
        if (frame.valid() && frame.is_dirty)
        {
            CHECK_WITH_LOG(
                commit(frame.file_id, frame), false,
                "commit frame failure\nfile_id: %d / frame pagenum: %ld",
                frame.file_id, frame.pagenum);
        }
    }
    return true;
}

bool BufferController::fsync(int file_id, bool free_flag)
{
    for (auto& frame : *buffer)
    {
        if (frame.valid() && frame.file_id == file_id)
        {
            if (frame.is_dirty)
            {
                CHECK_WITH_LOG(
                    commit(frame.file_id, frame), false,
                    "commit frame failure\nfile_id: %d / frame pagenum: %ld",
                    frame.file_id, frame.pagenum);
            }

            if (free_flag)
            {
                CHECK_WITH_LOG(frame.pin == 0, false,
                               "cannot free frame. frame.pin == %d", frame.pin);
                frame_free(index_table[file_id][frame.pagenum]);
            }
        }
    }
    return true;
}

int BufferController::frame_alloc()
{
    if (size() < capacity())
    {
        CHECK_WITH_LOG(size() == (capacity() - free_indexes->size()),
                       INVALID_BUFFER_INDEX,
                       "logical error. size: %ld / free_indexes size: %ld",
                       size(), free_indexes->size());
        int idx = free_indexes->top();
        free_indexes->pop();
        CHECK_WITH_LOG(!buffer->at(idx).valid(), INVALID_BUFFER_INDEX,
                       "free frame is valid!");
        return idx;
    }

    int index = select_victim<BufferLRUTraversalPolicy>();

    CHECK_WITH_LOG(index != INVALID_BUFFER_INDEX, INVALID_BUFFER_INDEX,
                   "alloc frame failure");
    CHECK_WITH_LOG(frame_free(index), INVALID_BUFFER_INDEX,
                   "frame free failure");

    return index;
}

bool BufferController::frame_free(int buffer_index)
{
    auto& frame = buffer->at(buffer_index);
    // 지금은 병렬 x
    if (frame.is_use_now())
    {
        frame.print_frame();
    }
    CHECK_WITH_LOG(!frame.is_use_now(), false, "concurrency not supported");

    while (frame.is_use_now())
    {
        // wait
    }

    CHECK(unlink_frame(buffer_index, frame));

    if (frame.is_dirty)
    {
        CHECK(commit(frame.file_id, frame));
    }

    forget_index(frame.file_id, frame.pagenum);
    frame.init();
    --num_buffer;

    return true;
}

int BufferController::load(int file_id, pagenum_t pagenum)
{
    auto& fileManager = getFileManager(file_id);
    int index = frame_alloc();
    CHECK_RET(index != INVALID_BUFFER_INDEX, INVALID_BUFFER_INDEX);
    auto& frame = buffer->at(index);

    frame_t page;
    CHECK_WITH_LOG(fileManager.load(pagenum, page), INVALID_BUFFER_INDEX,
                   "frame load failure: %ld", pagenum);
    frame.change_page(page);
    frame.file_id = file_id;
    frame.pagenum = pagenum;
    memorize_index(file_id, pagenum, index);
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
    return true;
}