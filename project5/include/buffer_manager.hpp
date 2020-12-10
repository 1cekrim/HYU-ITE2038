#ifndef __BUFFER_MANAGER_HPP__
#define __BUFFER_MANAGER_HPP__

#include <unistd.h>

#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stack>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <functional>

#include "file_manager.hpp"
#include "frame.hpp"
#include "logger.hpp"

struct node_tuple;

class BufferManager
{
 public:
    BufferManager();
    ~BufferManager();

    // node manager interface
    bool open(const std::string& name);
    // bool commit(pagenum_t pagenum, std::function<void(page_t&)> func, bool auto_release = true);
    // int load(pagenum_t pagenum, std::function<void(const page_t&)> func, bool auto_release = true);
    bool commit(pagenum_t pagenum, const page_t& page);
    int load(pagenum_t pagenum, page_t& page);
    pagenum_t create();
    void release(pagenum_t pagenum);
    void retain(pagenum_t pagenum);
    void release_shared(pagenum_t pagenum);
    void retain_shared(pagenum_t pagenum);
    bool free(pagenum_t pagenum);
    int get_manager_id() const;
    pagenum_t root() const;
    bool set_root(pagenum_t pagenum);
    bool close();

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

    // bool get(int file_id, pagenum_t pagenum, std::function<void(const page_t&)> func, bool auto_release = true);
    // bool put(int file_id, pagenum_t pagenum, std::function<void(page_t&)> func, bool auto_release = true);
    int get(int file_id, pagenum_t pagenum, page_t& frame);
    bool put(int file_id, pagenum_t pagenum, const page_t& frame);
    int create(int file_id);
    bool free(int file_id, pagenum_t pagenum);
    void release_frame(int file_id, pagenum_t pagenum);
    void retain_frame(int file_id, pagenum_t pagenum);
    void release_frame_shared(int file_id, pagenum_t pagenum);
    void retain_frame_shared(int file_id, pagenum_t pagenum);
    bool sync(bool lock = true);
    bool fsync(int file_id, bool free_flag = false);
    bool init_buffer(std::size_t buffer_size);
    bool clear_buffer();
    pagenum_t frame_id_to_pagenum(int frame_id);
    std::size_t size() const;
    std::size_t capacity() const;

    friend class BufferCircularLinearTraversalPolicy;
    friend class BufferLRUTraversalPolicy;
    friend void TEST_BUFFER();
    friend class node_tuple;
    
    std::mutex mtx;

 private:
    std::unique_ptr<std::vector<frame_t>> buffer;
    std::vector<std::unique_ptr<FileManager>> fileManagers;
    std::unordered_map<int, std::unordered_map<pagenum_t, int>> index_table;
    std::map<std::string, int> nameFileManagerMap;
    std::atomic<std::size_t> num_buffer;
    std::size_t buffer_size;
    int mru;
    int lru;
    bool valid_buffer_controller;
    std::unique_ptr<std::stack<int>> free_indexes;
    BufferController() : mru(INVALID_BUFFER_INDEX), lru(INVALID_BUFFER_INDEX)
    {
        // Do nothing
    }
    ~BufferController()
    {
        clear_buffer();
    }

    int find(int file_id, pagenum_t pagenum);
    void memorize_index(int file_id, pagenum_t pagenum, int frame_index);
    void forget_index(int file_id, pagenum_t pagenum);
    int frame_alloc();
    bool frame_free(int buffer_index, bool push_free_indexes_flag = true);
    bool update_recently_used(int buffer_index, frame_t& frame, bool unlink);
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