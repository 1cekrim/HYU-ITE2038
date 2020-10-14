#include "bptree.hpp"

#include <algorithm>
#include <cstring>
#include <iostream>
#include <vector>

#include "logger.hpp"

BPTree::BPTree(bool verbose_output, int delayed_min)
    : leaf_order(LEAF_ORDER),
      internal_order(INTERNAL_ORDER),
      verbose_output(verbose_output),
      delayed_min(delayed_min)
{
    // Do nothing
}

bool BPTree::open_table(const std::string& filename)
{
    CHECK_WITH_LOG(manager.open(filename), false, "open table failure");
    return true;
}

int BPTree::char_to_valType(valType& dst, const char* src) const
{
    std::fill(std::begin(dst), std::end(dst), 0);
    std::copy(src, src + std::max(static_cast<int>(strlen(src)), 119), dst);
    return 0;
}

int BPTree::get_table_id() const
{
    return manager.getFileDescriptor();
}

bool BPTree::insert(keyType key, const valType& value)
{
    // 중복을 허용하지 않음
    if (find(key))
    {
        return false;
    }

    auto record = make_record(key, value);

    if (!is_valid(manager.root()))
    {
        return start_new_tree(*record);
    }

    auto leaf = find_leaf(key);
    CHECK(leaf);

    if (leaf.node->number_of_keys() < leaf_order - 1)
    {
        return insert_into_leaf(leaf, *record);
    }

    return insert_into_leaf_after_splitting(leaf, *record);
}

bool BPTree::insert_into_leaf(node_tuple& leaf, const record_t& rec)
{
    int insertion_point =
        leaf.node->satisfy_condition_first<record_t>([&](auto& now) {
            return now.key >= rec.key;
        });

    leaf.node->insert(rec, insertion_point);

    CHECK(commit_node(leaf));

    return true;
}

bool BPTree::insert_into_leaf_after_splitting(node_tuple& leaf,
                                              const record_t& rec)
{
    auto new_leaf = node_tuple { create_node(), make_node(true) };
    CHECK(new_leaf);

    int insertion_index =
        leaf.node->satisfy_condition_first<record_t>([&](auto& now) {
            return now.key >= rec.key;
        });

    std::vector<record_t> temp;
    temp.reserve(leaf_order + 1);

    auto back = std::back_inserter(temp);
    leaf.node->range_copy<record_t>(back, 0, insertion_index);
    back = rec;
    leaf.node->range_copy<record_t>(back, insertion_index);

    int split = cut(leaf_order - 1);

    leaf.node->range_assignment<record_t>(temp.begin(), temp.begin() + split);
    new_leaf.node->range_assignment<record_t>(temp.begin() + split, temp.end());

    new_leaf.node->set_next_leaf(leaf.node->next_leaf());
    leaf.node->set_next_leaf(new_leaf.id);
    new_leaf.node->set_parent(leaf.node->parent());

    CHECK(commit_node(leaf));
    CHECK(commit_node(new_leaf));

    return insert_into_parent(leaf, new_leaf.node->get<record_t>(0).key,
                              new_leaf);
}

bool BPTree::insert_into_parent(node_tuple& left, keyType key,
                                node_tuple& right)
{
    auto parent_id = left.node->parent();
    if (!is_valid(parent_id))
    {
        return insert_into_new_root(left, key, right);
    }

    node_tuple parent { parent_id, load_node(parent_id) };
    CHECK(parent.node);

    int left_index = get_left_index(*parent.node, left.id);
    if (static_cast<int>(parent.node->nodePageHeader().numberOfKeys) <
        internal_order - 1)
    {
        return insert_into_node(parent, left_index, key, right);
    }

    return insert_into_node_after_splitting(parent, left_index, key, right);
}

int BPTree::get_left_index(const node_t& parent, nodeId_t left_id) const
{
    if (parent.leftmost() == left_id)
    {
        return 0;
    }

    int left_index = parent.satisfy_condition_first<internal_t>([&](auto& now) {
        return now.node_id == left_id;
    });
    return left_index + 1;
}

std::unique_ptr<node_t> BPTree::load_node(nodeId_t node_id)
{
    auto node = make_node();
    CHECK_RET(node, nullptr);

    node->load(manager, node_id);
    CHECK_WITH_LOG(node, nullptr, "load node failure: %ld", node_id);
    return node;
}

bool BPTree::commit_node(nodeId_t node_id, const node_t& node)
{
    CHECK_WITH_LOG(node.commit(manager, node_id), false,
                   "commit node failure: %ld", node_id);
    return true;
}

bool BPTree::commit_node(const node_tuple& target)
{
    CHECK_WITH_LOG(target.node->commit(manager, target.id), false,
                   "commit node failure: %ld", target.id);
    return true;
}

bool BPTree::free_node(const nodeId_t target)
{
    CHECK_WITH_LOG(manager.pageFree(target), false, "free page failure: %ld",
                   target);
    return true;
}

nodeId_t BPTree::create_node()
{
    nodeId_t t = manager.pageCreate();
    return t;
}

bool BPTree::insert_into_new_root(node_tuple& left, keyType key,
                                  node_tuple& right)
{
    node_tuple root = { create_node(), make_node() };
    CHECK(root);

    root.node->set_leftmost(left.id);
    root.node->emplace_back<internal_t>(key, right.id);

    left.node->set_parent(root.id);
    right.node->set_parent(root.id);

    CHECK(commit_node(left));
    CHECK(commit_node(right));
    CHECK(commit_node(root));

    CHECK(manager.set_root(root.id));

    return true;
}

bool BPTree::insert_into_node(node_tuple& parent, int left_index, keyType key,
                              node_tuple& right)
{
    parent.node->insert<internal_t>({ key, right.id }, left_index);
    CHECK(commit_node(parent));
    return true;
}

bool BPTree::insert_into_node_after_splitting(node_tuple& parent,
                                              int left_index, keyType key,
                                              node_tuple& target)
{
    node_tuple right = { create_node(), make_node() };
    CHECK(right);

    std::vector<internal_t> temp;
    temp.reserve(internal_order);

    auto back = std::back_inserter(temp);
    parent.node->range_copy<internal_t>(back, 0, left_index);
    back = { key, target.id };
    parent.node->range_copy<internal_t>(back, left_index);

    int split = cut(internal_order);

    keyType k_prime = temp[split - 1].key;

    parent.node->range_assignment<internal_t>(temp.begin(),
                                              temp.begin() + split - 1);

    right.node->set_leftmost(temp[split - 1].node_id);
    right.node->range_assignment<internal_t>(temp.begin() + split, temp.end());
    right.node->set_parent(parent.node->parent());

    CHECK(update_parent_with_commit(right.node->leftmost(), right.id));
    for (auto& tmp : right.node->range<internal_t>())
    {
        CHECK(update_parent_with_commit(tmp.node_id, right.id));
    }

    CHECK(commit_node(parent));
    CHECK(commit_node(right));

    return insert_into_parent(parent, k_prime, right);
}

bool BPTree::delete_key(keyType key)
{
    auto record = find(key);
    auto leaf = find_leaf(key);
    CHECK(leaf);
    CHECK(record);

    CHECK_WITH_LOG(delete_entry(leaf, key), false, "delete entry failure");

    return true;
}

bool BPTree::delete_entry(node_tuple& target, keyType key)
{
    CHECK_WITH_LOG(remove_entry_from_node(target, key), false,
                   "remove entry from node failure: %ld", key);
    CHECK(commit_node(target));

    if (target.id == manager.root())
    {
        return adjust_root();
    }

    int min_keys;
    if (delayed_min)
    {
        min_keys = delayed_min;
    }
    else
    {
        min_keys =
            target.node->is_leaf() ? cut(leaf_order - 1) : cut(leaf_order) - 1;
    }

    if (static_cast<int>(target.node->number_of_keys()) >= min_keys)
    {
        return true;
    }

    node_tuple parent = { target.node->parent(),
                          load_node(target.node->parent()) };
    CHECK(parent);
    CHECK(parent.node->number_of_keys() > 0);

    int neighbor_index = get_left_index(*parent.node, target.id) - 1;
    int k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
    keyType k_prime = parent.node->get<internal_t>(k_prime_index).key;

    node_tuple neighbor;
    neighbor.id =
        neighbor_index == -1
            ? parent.node->get<internal_t>(0).node_id
            : neighbor_index == 0
                  ? parent.node->leftmost()
                  : parent.node->get<internal_t>(neighbor_index - 1).node_id;
    neighbor.node = load_node(neighbor.id);
    CHECK(neighbor);

    int capacity = target.node->is_leaf() ? leaf_order : internal_order - 1;

    if (static_cast<int>(target.node->number_of_keys() +
                         neighbor.node->number_of_keys()) < capacity)
    {
        node_tuple tmp_neightbor, tmp_target;
        if (neighbor_index == -1)
        {
            tmp_neightbor = std::move(target);
            tmp_target = std::move(neighbor);
        }
        else
        {
            tmp_neightbor = std::move(neighbor);
            tmp_target = std::move(target);
        }
        return coalesce_nodes(tmp_target, tmp_neightbor, parent, k_prime);
    }
    else
    {
        return redistribute_nodes(target, neighbor, parent, k_prime,
                                  k_prime_index, neighbor_index);
    }
}

bool BPTree::remove_entry_from_node(node_tuple& target, keyType key)
{
    if (target.node->is_leaf())
    {
        int idx =
            target.node->satisfy_condition_first<record_t>([&](auto& now) {
                return now.key == key;
            });
        CHECK_WITH_LOG(idx != target.node->number_of_keys(), false,
                       "invalid key: %ld", key);
        target.node->erase<record_t>(idx);
    }
    else
    {
        int idx =
            target.node->satisfy_condition_first<internal_t>([&](auto& now) {
                return now.key == key;
            });
        CHECK_WITH_LOG(idx != target.node->number_of_keys(), false,
                       "invalid key: %ld", key);
        target.node->erase<internal_t>(idx);
    }

    return true;
}

bool BPTree::adjust_root()
{
    node_tuple root = { manager.root(), load_node(manager.root()) };
    CHECK(root);

    if (root.node->number_of_keys() > 0)
    {
        return true;
    }

    if (root.node->is_leaf())
    {
        CHECK(manager.set_root(INVALID_NODE_ID));
    }
    else
    {
        nodeId_t new_root_id = root.node->leftmost();
        CHECK(manager.set_root(new_root_id));

        CHECK(update_parent_with_commit(new_root_id, INVALID_NODE_ID));
    }

    CHECK(free_node(root.id));

    return true;
}

bool BPTree::update_parent_with_commit(nodeId_t target_id, nodeId_t parent_id)
{
    auto temp = load_node(target_id);
    CHECK(temp);
    temp->set_parent(parent_id);
    CHECK(commit_node(target_id, *temp));

    return true;
}

bool BPTree::coalesce_nodes(node_tuple& target, node_tuple& neighbor,
                            node_tuple& parent, int k_prime)
{
    if (target.node->is_leaf())
    {
        for (auto& rec : target.node->range<record_t>())
        {
            neighbor.node->push_back(rec);
        }
        neighbor.node->set_next_leaf(target.node->next_leaf());
    }
    else
    {
        neighbor.node->emplace_back<internal_t>(k_prime,
                                                target.node->leftmost());
        CHECK(update_parent_with_commit(target.node->leftmost(), neighbor.id));
        for (auto& internal : target.node->range<internal_t>())
        {
            neighbor.node->push_back(internal);

            CHECK(update_parent_with_commit(internal.node_id, neighbor.id));
        }
    }

    CHECK(commit_node(neighbor));
    CHECK(delete_entry(parent, k_prime));
    CHECK(free_node(target.id));

    return true;
}

bool BPTree::redistribute_nodes(node_tuple& target, node_tuple& neighbor,
                                node_tuple& parent, int k_prime,
                                int k_prime_index, int neighbor_index)
{
    if (neighbor_index != -1)
    {
        // left neighbor
        if (!target.node->is_leaf())
        {
            target.node->insert<internal_t>(
                { k_prime, target.node->leftmost() }, 0);

            auto& new_one = neighbor.node->back<internal_t>();

            parent.node->get<internal_t>(k_prime_index).key = new_one.key;
            target.node->set_leftmost(new_one.node_id);

            CHECK(update_parent_with_commit(new_one.node_id, target.id));
        }
        else
        {
            target.node->insert(neighbor.node->back<record_t>(), 0);

            parent.node->get<record_t>(k_prime_index).key =
                target.node->first<record_t>().key;
        }
    }
    else
    {
        // right neighbor
        if (!target.node->is_leaf())
        {
            target.node->emplace_back<internal_t>(k_prime,
                                                  neighbor.node->leftmost());

            auto& leftmost = neighbor.node->first<internal_t>();
            parent.node->get<internal_t>(k_prime_index).key = leftmost.key;
            neighbor.node->set_leftmost(leftmost.node_id);

            CHECK(update_parent_with_commit(
                target.node->back<internal_t>().node_id, target.id));

            target.node->erase<internal_t>(0);
        }
        else
        {
            target.node->push_back(neighbor.node->first<record_t>());

            parent.node->get<record_t>(k_prime_index).key =
                neighbor.node->get<record_t>(1).key;

            neighbor.node->erase<record_t>(0);
        }
    }

    CHECK(commit_node(target));
    CHECK(commit_node(neighbor));
    CHECK(commit_node(parent));

    return true;
}

std::unique_ptr<record_t> BPTree::find(keyType key)
{
    auto leaf = find_leaf(key);
    if (!leaf)
    {
        return nullptr;
    }

    int i;
    for (i = 0; i < leaf.node->number_of_keys(); ++i)
    {
        if (leaf.node->get<record_t>(i).key == key)
        {
            break;
        }
    }

    if (i == leaf.node->number_of_keys())
    {
        return nullptr;
    }
    else
    {
        return std::make_unique<record_t>(leaf.node->get<record_t>(i));
    }
}

node_tuple BPTree::find_leaf(keyType key)
{
    auto root_id = manager.root();
    if (!is_valid(root_id))
    {
        return node_tuple::invalid();
    }

    node_tuple root = { root_id, load_node(root_id) };
    CHECK_RET(root, node_tuple::invalid());

    while (!root.node->is_leaf())
    {
        if (verbose_output)
        {
            std::cout << '[';
            for (auto& internal : root.node->range<internal_t>())
            {
                std::cout << internal.key << ' ';
            }
            std::cout << "] \n";
        }

        int idx =
            root.node->satisfy_condition_first<internal_t>([&](auto& now) {
                return now.key > key;
            }) -
            1;

        if (verbose_output)
        {
            printf("%d ->\n", idx);
        }

        root.node = load_node(
            root.id = (idx == -1) ? root.node->leftmost()
                                  : root.node->get<internal_t>(idx).node_id);
    }

    if (verbose_output)
    {
        std::cout << "Leaf [";
        for (auto& record : root.node->range<record_t>())
        {
            std::cout << record.key << " ";
        }
        std::cout << "] ->\n";
    }

    return root;
}

std::unique_ptr<record_t> BPTree::make_record(keyType key,
                                              const valType& value) const
{
    auto rec = std::make_unique<record_t>();
    CHECK_RET(rec, nullptr);
    rec->init(key, value);

    return rec;
}

std::unique_ptr<node_t> BPTree::make_node(bool is_leaf) const
{
    auto n = std::make_unique<node_t>();
    CHECK_WITH_LOG(n, nullptr, "allocation failure: node");

    auto& header = n->nodePageHeader();

    header.isLeaf = is_leaf;

    return n;
}

bool BPTree::start_new_tree(const record_t& rec)
{
    node_tuple root = { create_node(), make_node(true) };
    CHECK(root);

    root.node->insert(rec, 0);

    CHECK(commit_node(root));
    CHECK(manager.set_root(root.id));

    return true;
}

bool BPTree::is_valid(nodeId_t node_id) const
{
    return node_id != INVALID_NODE_ID;
}