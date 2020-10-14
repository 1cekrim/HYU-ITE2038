#ifndef __BPTREE_HPP__
#define __BPTREE_HPP__

#include <memory>
#include <string_view>

#include "file_manager.hpp"
#include "page.hpp"

constexpr auto LEAF_ORDER = 32;
constexpr auto INTERNAL_ORDER = 249;
constexpr auto VERBOSE_OUTPUT = false;
constexpr auto DELAYED_MIN = 1;
constexpr auto INVALID_NODE_ID = EMPTY_PAGE_NUMBER;

using node_t = page_t;
using nodeId_t = pagenum_t;
using record_t = Record;
using internal_t = Internal;
using manager_t = FileManager;

struct node_tuple
{
    nodeId_t id;
    std::unique_ptr<node_t> node;
    static node_tuple invalid()
    {
        return { EMPTY_PAGE_NUMBER, nullptr };
    }
    operator bool() const
    {
        return id != INVALID_NODE_ID && node;
    }
};

class BPTree
{
 public:
    BPTree(bool verbose_output = VERBOSE_OUTPUT, int delayed_min = DELAYED_MIN);
    int char_to_valType(valType& dst, const char* src) const;
    int get_table_id() const;
    bool open_table(const std::string& filename);
    // Insertion.
    bool insert(keyType key, const valType& value);
    bool delete_key(keyType key);

    std::unique_ptr<record_t> find(keyType key);
    node_tuple find_leaf(keyType key);

 private:
    std::unique_ptr<record_t> make_record(keyType key,
                                          const valType& value) const;
    std::unique_ptr<node_t> make_node(bool is_leaf = false) const;
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
    bool adjust_root();
    bool coalesce_nodes(node_tuple& target, node_tuple& neighbor,
                        node_tuple& parent, int k_prime);
    bool redistribute_nodes(node_tuple& target, node_tuple& neighbor,
                            node_tuple& parent, int k_prime, int k_prime_index,
                            int neighbor_index);
    bool update_parent_with_commit(nodeId_t target_id, nodeId_t parent_id);
    std::unique_ptr<node_t> load_node(nodeId_t node_id);
    bool commit_node(nodeId_t page, const node_t& n);
    bool commit_node(const node_tuple& nt);
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
    int internal_order;
    bool verbose_output;
    int delayed_min;
};

#endif /* __BPTREE_HPP__*/
