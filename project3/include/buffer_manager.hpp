#ifndef __BUFFER_MANAGER_HPP__
#define __BUFFER_MANAGER_HPP__

#include <unistd.h>

#include <cstdio>
#include <map>
#include <memory>
#include <string_view>
#include <vector>

#include "file_manager.hpp"
#include "frame.hpp"
#include "logger.hpp"

constexpr auto BUFFER_SIZE = 10;

class BufferManager
{
 public:
    BufferManager();
    ~BufferManager();

    // node manager interface
    bool open(const std::string& name);
    bool commit(pagenum_t pagenum, const frame_t& frame);
    int load(pagenum_t pagenum, frame_t& frame);
    pagenum_t create();
    bool free(pagenum_t pagenum);
    int get_manager_id() const;
    pagenum_t root() const;
    bool set_root(pagenum_t pagenum);

    const HeaderPageHeader& getFileHeader() const;

 private:
    FileManager* fileManager;
    int manager_id = -1;
};

class BufferController
{
 public:
    static BufferController& instance()
    {
        static BufferController bufferController;
        return bufferController;
    }
    int openFileManager(const std::string& name);
    FileManager& getFileManager(int file_id);

    int get(int file_id, pagenum_t pagenum, frame_t& frame);
    bool put(int file_id, pagenum_t pagenum, const frame_t& frame);
    int create(int file_id);
    bool free(int file_id, pagenum_t pagenum);
    void release_frame(int frame_index);
    void retain_frame(int frame_index);
    bool sync();
    pagenum_t frame_id_to_pagenum(int frame_id) const;
    std::size_t size() const;
    std::size_t capacity() const;

    friend class BufferCircularLinearTraversalPolicy;
    friend class BufferLRUTraversalPolicy;
    friend void TEST_BUFFER();
    
 private:
    std::unique_ptr<std::vector<frame_t>> buffer;
    std::vector<std::unique_ptr<FileManager>> fileManagers;
    std::map<std::string, int> nameFileManagerMap;
    std::size_t num_buffer;
    int mru;
    int lru;
    BufferController()
        : buffer(std::make_unique<std::vector<frame_t>>(BUFFER_SIZE)),
          mru(INVALID_BUFFER_INDEX),
          lru(INVALID_BUFFER_INDEX)
    {
        for (auto& frame : *buffer)
        {
            frame.init();
        }
    }

    int find(int file_id, pagenum_t pagenum);
    int frame_alloc();
    bool frame_free(int buffer_index);
    bool update_recently_used(int buffer_index, frame_t& frame);
    bool unlink_frame(int buffer_index, frame_t& frame);

    template<typename Policy>
    int select_victim()
    {
        for (auto it = Policy::begin(); it != Policy::end(); ++it)
        {
            if (!it->is_use_now())
            {
                return it.frame_index();
            }
        }
        CHECK_WITH_LOG(false, INVALID_BUFFER_INDEX, "select victim failure");
    }

    int load(int file_id, pagenum_t pagenum);
    bool commit(int file_id, const frame_t& frame);
};

class BufferLRUTraversalPolicy
{
 public:
    class iterator
    {
     public:
        iterator(int ptr) : ptr(ptr)
        {
            // Do nothing
        }

        int frame_index() const
        {
            return ptr;
        }

        bool operator==(const iterator& rhs) const
        {
            return rhs.ptr == ptr;
        }

        bool operator!=(const iterator& rhs) const
        {
            return rhs.ptr != ptr;
        }

        frame_t* operator->()
        {
            return &BufferController::instance().buffer->at(ptr);
        }

        void operator++()
        {
            ptr = BufferController::instance().buffer->at(ptr).next;
            if (ptr == -1)
            {
                ptr = BufferController::instance().lru;
            }
        }
        void operator--() = delete;
        void operator++(int) = delete;
        void operator--(int) = delete;

        frame_t& operator*()
        {
            return BufferController::instance().buffer->at(ptr);
        }

     private:
        int ptr;
    };
    static iterator begin()
    {
        return iterator(BufferController::instance().lru);
    }

    static iterator end()
    {
        return iterator(BufferController::instance().capacity());
    }

    static frame_t& front()
    {
        return *begin();
    }

    static bool empty()
    {
        // Traversal Policy
        return false;
    }

    static std::size_t size()
    {
        // Traversal Policy
        return BufferController::instance().buffer->size();
    }
};

// TODO: unittest BufferCircularLinearTraversalPolicy
class BufferCircularLinearTraversalPolicy
{
 public:
    using It = std::vector<frame_t>::iterator;
    int now_index() const
    {
        return ptr;
    }

    It begin() const
    {
        return buffer.begin() + ptr;
    }

    It end() const
    {
        return buffer.end();
    }

    bool empty() const
    {
        // Traversal Policy
        return false;
    }

    std::size_t size() const
    {
        // Traversal Policy
        return buffer.size();
    }

    frame_t& front() const
    {
        return *begin();
    }

    frame_t& back() const
    {
        return *(std::prev(end()));
    }

    void operator++()
    {
        ptr = (ptr + 1) % buffer.size();
    }

    void operator--()
    {
        ptr = (ptr - 1 + buffer.size()) % buffer.size();
    }

    void operator++(int) = delete;
    void operator--(int) = delete;

    frame_t& operator*()
    {
        return front();
    }

    BufferCircularLinearTraversalPolicy()
        : buffer(*BufferController::instance().buffer),
          ptr(0)
    {
        // Do nothing
    }

 private:
    std::vector<frame_t>& buffer;

    int ptr;
};

#endif /* __BUFFER_MANAGER_HPP__*/