#ifndef __BPT_HPP__
#define __BPT_HPP__

#include <memory>

#include "file_manager.hpp"
#include "page.hpp"

constexpr auto LEAF_ORDER = 32;
constexpr auto INTERNAL_ORDER = 249;
constexpr auto VERBOSE_OUTPUT = false;

using node = page_t;
using record = Record;

struct node_tuple
{
    std::unique_ptr<node> n;
    pagenum_t pagenum;
    static node_tuple invalid()
    {
        return { nullptr, EMPTY_PAGE_NUMBER };
    }
};

class BPTree
{
 public:
    BPTree(FileManager& fm, bool verbose_output = VERBOSE_OUTPUT);
    int char_to_valType(valType& dst, const char* src) const;

    // Insertion.
    bool insert(keyType key, const valType& value);
    std::unique_ptr<record> find(keyType key);
    node_tuple find_leaf(keyType key);

 private:
    std::unique_ptr<record> make_record(keyType key,
                                        const valType& value) const;
    std::unique_ptr<node> make_node(bool is_leaf) const;
    bool insert_into_leaf(node_tuple& leaf, const record& rec);
    bool insert_into_leaf_after_splitting(node_tuple& leaf_tuple,
                                          const record& rec);
    bool insert_into_new_root(node_tuple& left, keyType key, node_tuple& right);
    bool insert_into_node(node_tuple& parent, int left_index, keyType key, node_tuple& right);
    bool insert_into_node_after_splitting(node_tuple& parent, int left_index, keyType key, node_tuple& right);
    bool insert_into_parent(node_tuple& left, keyType key, node_tuple& right);
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
    FileManager& fm;
    int leaf_order;
    int internal_order;
    bool verbose_output;
};

#endif /* __BPT_HPP__*/
