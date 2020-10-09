#ifndef __BPT_H__
#define __BPT_H__

#include <memory>

#include "file_manager.hpp"
#include "page.hpp"

constexpr auto LEAF_ORDER = 32;
constexpr auto INTERNAL_ORDER = 249;
constexpr auto VERBOSE_OUTPUT = false;

using node = page_t;
using record = Record;

class BPTree
{
 public:
    BPTree(FileManager& fm);

    // Insertion.
    bool insert(keyType key, const val_t& value);
    std::unique_ptr<record> find(keyType key);
    std::unique_ptr<node> find_leaf(keyType key);

 private:
    std::unique_ptr<record> make_record(keyType key, const val_t& value) const;
    std::unique_ptr<node> make_node(bool is_leaf) const;
    bool start_new_tree(std::unique_ptr<record> record);
    FileManager& fm;
    int leaf_order;
    int internal_order;
    bool verbose_output;
};

#endif /* __BPT_H__*/
