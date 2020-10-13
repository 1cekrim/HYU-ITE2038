#ifndef __BPT_HPP__
#define __BPT_HPP__

#include <memory>
#include <string_view>

#include "file_manager.hpp"
#include "page.hpp"

constexpr auto LEAF_ORDER = 32;
constexpr auto INTERNAL_ORDER = 249;
constexpr auto VERBOSE_OUTPUT = false;
constexpr auto DELAYED_MIN = 1;

using node = page_t;
using record = Record;

struct node_tuple
{
    pagenum_t pagenum;
    std::unique_ptr<node> n;
    static node_tuple invalid()
    {
        return { EMPTY_PAGE_NUMBER, nullptr };
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

    std::unique_ptr<record> find(keyType key);
    node_tuple find_leaf(keyType key);

 private:
    std::unique_ptr<record> make_record(keyType key,
                                        const valType& value) const;
    std::unique_ptr<node> make_node(bool is_leaf = false) const;
    bool insert_into_leaf(node_tuple& leaf, const record& rec);
    bool insert_into_leaf_after_splitting(node_tuple& leaf_tuple,
                                          const record& rec);
    bool insert_into_new_root(node_tuple& left, keyType key, node_tuple& right);
    bool insert_into_node(node_tuple& parent, int left_index, keyType key,
                          node_tuple& right);
    bool insert_into_node_after_splitting(node_tuple& parent, int left_index,
                                          keyType key, node_tuple& right);
    bool insert_into_parent(node_tuple& left, keyType key, node_tuple& right);

    bool delete_entry(node_tuple& target, keyType key);
    bool remove_entry_from_node(node_tuple& target, keyType key);
    bool adjust_root();
    bool coalesce_nodes(node_tuple& target_tuple, node_tuple& neighbor_tuple,
                        node_tuple& parent_tuple, int k_prime);
    bool redistribute_nodes(node_tuple& target_tuple,
                            node_tuple& neighbor_tuple,
                            node_tuple& parent_tuple, int k_prime,
                            int k_prime_index, int neighbor_index);
    bool update_parent_with_commit(pagenum_t target, pagenum_t parent);
    std::unique_ptr<node> load_node(pagenum_t pagenum);
    bool commit_node(pagenum_t page, const node& n);
    bool commit_node(const node_tuple& nt);

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
    int get_left_index(const node& parent, pagenum_t left) const;
    bool start_new_tree(const record& rec);
    FileManager fm;
    int leaf_order;
    int internal_order;
    bool verbose_output;
    int delayed_min;
};

#endif /* __BPT_HPP__*/
