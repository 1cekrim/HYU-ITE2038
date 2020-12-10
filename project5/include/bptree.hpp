#ifndef __BPTREE_HPP__
#define __BPTREE_HPP__

#include <iostream>
#include <memory>
#include <string_view>

#include "buffer_manager.hpp"
#include "frame.hpp"
#include "log_manager.hpp"
#include "page.hpp"
#include "transaction_manager.hpp"

constexpr auto LEAF_ORDER = 32;
constexpr auto INTERNAL_ORDER = 249;
constexpr auto VERBOSE_OUTPUT = false;
constexpr auto DELAYED_MIN = 1;
constexpr auto INVALID_NODE_ID = EMPTY_PAGE_NUMBER;

using node_t = page_t;
using nodeId_t = pagenum_t;
using record_t = Record;
using internal_t = Internal;
using manager_t = BufferManager;

struct node_tuple
{
    nodeId_t id;
    node_t node;
    node_tuple() = default;
    node_tuple(const node_tuple&) = delete;
    node_tuple(node_tuple&&) = delete;
    node_tuple& operator=(const node_tuple&) = delete;
    node_tuple& operator=(node_tuple&&) = delete;

    operator bool() const
    {
        return id != INVALID_NODE_ID;
    }
};

class scoped_node_latch
{
 public:
    scoped_node_latch(int manager_id, nodeId_t id);
    ~scoped_node_latch();
    void lock();
    void unlock();

 private:
    int manager_id;
    nodeId_t id;
};

class scoped_node_latch_shared
{
 public:
    scoped_node_latch_shared(int manager_id, nodeId_t id);
    ~scoped_node_latch_shared();
    void lock_shared();
    void unlock_shared();

 private:
    int manager_id;
    nodeId_t id;
};

class BPTree
{
 public:
    BPTree(bool verbose_output = VERBOSE_OUTPUT, int delayed_min = DELAYED_MIN);
    void set_table(int table_id);
    int char_to_valType(valType& dst, const char* src) const;
    int get_table_id() const;
    bool open_table(const std::string& filename);
    // Insertion.
    bool insert(keyType key, const valType& value);
    bool update(
        keyType key, const valType& value,
        int transaction_id = TransactionManager::invliad_transaction_id);
    bool delete_key(keyType key);

    bool find(keyType key, record_t& ret,
              int transaction_id = TransactionManager::invliad_transaction_id);

 private:
    bool find_leaf(keyType key, node_tuple& ret);
    bool exist_key(keyType key);
    bool insert_into_leaf(node_tuple& leaf, const record_t& rec);
    bool insert_into_leaf_after_splitting(node_tuple& leaf,
                                          const record_t& rec);
    bool insert_into_new_root(node_tuple& left, keyType key, node_tuple& right);
    bool insert_into_node(node_tuple& parent, int left_index, keyType key,
                          node_tuple& right);
    bool insert_into_node_after_splitting(node_tuple& parent, int left_index,
                                          keyType key, node_tuple& right);
    bool insert_into_parent(node_tuple& left, keyType key, node_tuple& right);

    bool delete_entry(node_tuple& target, keyType key);
    bool remove_entry_from_node(node_tuple& target, keyType key);
    bool adjust_root(node_tuple& root);
    bool coalesce_nodes(node_tuple& target, node_tuple& neighbor,
                        node_tuple& parent, int k_prime);
    bool redistribute_nodes(node_tuple& target, node_tuple& neighbor,
                            node_tuple& parent, int k_prime, int k_prime_index,
                            int neighbor_index);
    bool update_parent_with_commit(nodeId_t target_id, nodeId_t parent_id);
    bool load_node(node_tuple& target);
    bool commit_node(nodeId_t page, const node_t& n);
    bool commit_node(const node_tuple& target);
    bool free_node(node_tuple& target);
    nodeId_t create_node();
    bool is_valid(nodeId_t node_id) const;

    constexpr int cut(int length)
    {
        if (length % 2)
        {
            return length / 2 + 1;
        }
        else
        {
            return length / 2;
        }
    }
    int get_left_index(const node_t& parent, nodeId_t left) const;
    bool start_new_tree(const record_t& rec);
    manager_t manager;
    int leaf_order;
    int table_id;
    int internal_order;
    bool verbose_output;
    int delayed_min;
};

#endif /* __BPTREE_HPP__*/
