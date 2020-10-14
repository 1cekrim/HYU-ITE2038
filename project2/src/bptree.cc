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

    commit_node(leaf);

    return true;
}

bool BPTree::insert_into_leaf_after_splitting(node_tuple& leaf,
                                              const record_t& rec)
{
    auto new_leaf = node_tuple { create_node(), make_node(true) };

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

    return insert_into_parent(leaf, new_leaf.node->get<Record>(0).key,
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
    node->load(manager, node_id);
    EXIT_WITH_LOG(node, "load node failure: %ld", node_id);
    return node;
}

bool BPTree::commit_node(nodeId_t node_id, const node_t& node)
{
    EXIT_WITH_LOG(node.commit(manager, node_id), "commit node failure: %ld",
                  node_id);
}

bool BPTree::commit_node(const node_tuple& nt)
{
    EXIT_WITH_LOG(nt.node->commit(manager, nt.id), "commit node failure: %ld",
                  nt.id);
}

nodeId_t BPTree::create_node()
{
    nodeId_t t = manager.pageCreate();
    EXIT_WITH_LOG(is_valid(t), "create page failure");
    return t;
}

bool BPTree::insert_into_new_root(node_tuple& left, keyType key,
                                  node_tuple& right)
{
    node_tuple root = { create_node(), make_node() };

    root.node->set_leftmost(left.id);
    root.node->emplace_back<internal_t>(key, right.id);

    left.node->set_parent(root.id);
    right.node->set_parent(root.id);

    CHECK(commit_node(left));
    CHECK(commit_node(right));
    CHECK(commit_node(root));

    manager.set_root(root.id);

    return true;
}

bool BPTree::insert_into_node(node_tuple& parent, int left_index, keyType key,
                              node_tuple& right)
{
    parent.node->insert<Internal>({ key, right.id }, left_index);
    CHECK(commit_node(parent));
    return true;
}

bool BPTree::insert_into_node_after_splitting(node_tuple& parent,
                                              int left_index, keyType key,
                                              node_tuple& target)
{
    node_tuple right = { create_node(), make_node() };

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

    commit_node(parent);
    commit_node(right);

    return insert_into_parent(parent, k_prime, right);
}

bool BPTree::delete_key(keyType key)
{
    auto record = find(key);
    auto leaf = find_leaf(key);
    CHECK_WITH_LOG(is_valid(leaf.id), false, "find leaf failure: %ld", key);
    if (!record)
    {
        return false;
    }

    CHECK_WITH_LOG(delete_entry(leaf, key), false, "delete entry failure");

    return true;
}

bool BPTree::delete_entry(node_tuple& target_tuple, keyType key)
{
    CHECK_WITH_LOG(remove_entry_from_node(target_tuple, key), false,
                   "remove entry from node failure: %ld", key);
    commit_node(target_tuple);

    if (target_tuple.id == manager.root())
    {
        return adjust_root();
    }

    auto& target_header = target_tuple.node->nodePageHeader();

    int min_keys;
    if (delayed_min)
    {
        min_keys = delayed_min;
    }
    else
    {
        min_keys =
            target_header.isLeaf ? cut(leaf_order - 1) : cut(leaf_order) - 1;
    }

    if (static_cast<int>(target_header.numberOfKeys) >= min_keys)
    {
        return true;
    }

    auto parent = load_node(target_header.parentPageNumber);
    CHECK(parent);
    auto& parent_header = parent->nodePageHeader();
    CHECK_WITH_LOG(parent_header.numberOfKeys != 0, false,
                   "empty parent: %ld. child: %ld",
                   target_header.parentPageNumber, target_tuple.id);
    auto& parent_internals = parent->internals();

    int neighbor_index = get_left_index(*parent, target_tuple.id) - 1;
    int k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
    keyType k_prime = parent_internals[k_prime_index].key;

    node_tuple neighbor_tuple;
    neighbor_tuple.id =
        neighbor_index == -1
            ? parent_internals[0].node_id
            : neighbor_index == 0
                  ? parent_header.onePageNumber
                  : parent_internals[neighbor_index - 1].node_id;
    neighbor_tuple.node = load_node(neighbor_tuple.id);
    CHECK(neighbor_tuple.node);

    int capacity = target_header.isLeaf ? leaf_order : internal_order - 1;

    node_tuple parent_tuple { target_header.parentPageNumber,
                              std::move(parent) };

    if (static_cast<int>(target_header.numberOfKeys +
                         neighbor_tuple.node->nodePageHeader().numberOfKeys) <
        capacity)
    {
        node_tuple tmp_neightbor, tmp_target;
        if (neighbor_index == -1)
        {
            tmp_neightbor = std::move(target_tuple);
            tmp_target = std::move(neighbor_tuple);
        }
        else
        {
            tmp_neightbor = std::move(neighbor_tuple);
            tmp_target = std::move(target_tuple);
        }
        return coalesce_nodes(tmp_target, tmp_neightbor, parent_tuple, k_prime);
    }
    else
    {
        return redistribute_nodes(target_tuple, neighbor_tuple, parent_tuple,
                                  k_prime, k_prime_index, neighbor_index);
    }
}

bool BPTree::remove_entry_from_node(node_tuple& target, keyType key)
{
    auto& header = target.node->nodePageHeader();
    if (header.isLeaf)
    {
        int idx =
            target.node->satisfy_condition_first<record_t>([&](auto& now) {
                return now.key == key;
            });
        CHECK_WITH_LOG(idx != static_cast<int>(header.numberOfKeys), false,
                       "invalid key: %ld", key);
        target.node->erase<record_t>(idx);
    }
    else
    {
        int idx =
            target.node->satisfy_condition_first<internal_t>([&](auto& now) {
                return now.key == key;
            });
        CHECK_WITH_LOG(idx != static_cast<int>(header.numberOfKeys), false,
                       "invalid key: %ld", key);
        target.node->erase<internal_t>(idx);
    }

    return true;
}

bool BPTree::adjust_root()
{
    node_tuple root = { manager.root(), load_node(manager.root()) };

    if (root.node->number_of_keys() > 0)
    {
        return true;
    }

    if (root.node->is_leaf())
    {
        manager.set_root(INVALID_NODE_ID);
    }
    else
    {
        nodeId_t new_root_id = root.node->leftmost();
        manager.set_root(new_root_id);

        CHECK(update_parent_with_commit(new_root_id, INVALID_NODE_ID));
    }

    CHECK_WITH_LOG(manager.pageFree(root.id), false,
                   "free old root page failure");

    return true;
}

bool BPTree::update_parent_with_commit(nodeId_t target_pagenum,
                                       nodeId_t parent_pagenum)
{
    auto temp = load_node(target_pagenum);
    temp->nodePageHeader().parentPageNumber = parent_pagenum;
    CHECK(commit_node(target_pagenum, *temp));
}

bool BPTree::coalesce_nodes(node_tuple& target_tuple,
                            node_tuple& neighbor_tuple,
                            node_tuple& parent_tuple, int k_prime)
{
    auto& target_header = target_tuple.node->nodePageHeader();
    auto& neighbor_header = neighbor_tuple.node->nodePageHeader();

    if (target_header.isLeaf)
    {
        auto& neighbor = neighbor_tuple.node;
        for (auto& rec : target_tuple.node->range<record_t>())
        {
            neighbor->push_back(rec);
        }
        neighbor_header.onePageNumber = target_header.onePageNumber;
    }
    else
    {
        // internal
        auto& neighbor = neighbor_tuple.node;

        nodeId_t pagenum;
        neighbor->emplace_back<internal_t>(
            k_prime, pagenum = target_header.onePageNumber);
        CHECK(update_parent_with_commit(pagenum, neighbor_tuple.id));
        for (auto& internal : target_tuple.node->range<internal_t>())
        {
            pagenum = internal.node_id;
            neighbor->push_back(internal);

            CHECK(update_parent_with_commit(pagenum, neighbor_tuple.id));
        }
    }

    commit_node(neighbor_tuple);
    CHECK_WITH_LOG(delete_entry(parent_tuple, k_prime), false,
                   "delete entry of parent failure: %ld", parent_tuple.id);
    CHECK_WITH_LOG(manager.pageFree(target_tuple.id), false,
                   "free target page failure: %ld", target_tuple.id);

    return true;
}

bool BPTree::redistribute_nodes(node_tuple& target_tuple,
                                node_tuple& neighbor_tuple,
                                node_tuple& parent_tuple, int k_prime,
                                int k_prime_index, int neighbor_index)
{
    auto& target_header = target_tuple.node->nodePageHeader();
    auto& neighbor_header = neighbor_tuple.node->nodePageHeader();
    auto& parent_internals = parent_tuple.node->internals();

    if (neighbor_index != -1)
    {
        // left neighbor
        if (!target_header.isLeaf)
        {
            auto& neighbor_internals = neighbor_tuple.node->internals();

            target_tuple.node->insert<internal_t>(
                { k_prime, target_header.onePageNumber }, 0);

            Internal& new_one =
                neighbor_internals[neighbor_header.numberOfKeys - 1];
            parent_internals[k_prime_index].key = new_one.key;
            target_header.onePageNumber = new_one.node_id;

            CHECK(update_parent_with_commit(new_one.node_id, target_tuple.id));
        }
        else
        {
            auto& neighbor_records = neighbor_tuple.node->records();
            auto& target_records = target_tuple.node->records();

            target_tuple.node->insert(
                neighbor_records[neighbor_header.numberOfKeys - 1], 0);

            parent_internals[k_prime_index].key = target_records[0].key;
        }
    }
    else
    {
        // right neighbor
        if (!target_header.isLeaf)
        {
            auto& neighbor_internals = neighbor_tuple.node->internals();
            auto& target_internals = target_tuple.node->internals();

            target_tuple.node->emplace_back<internal_t>(
                k_prime, neighbor_header.onePageNumber);

            parent_internals[k_prime_index].key = neighbor_internals[0].key;
            neighbor_header.onePageNumber = neighbor_internals[0].node_id;

            CHECK(update_parent_with_commit(
                target_internals[target_header.numberOfKeys - 1].node_id,
                target_tuple.id));

            neighbor_tuple.node->erase<internal_t>(0);
        }
        else
        {
            auto& neighbor_records = neighbor_tuple.node->records();

            target_tuple.node->push_back(neighbor_records[0]);

            parent_internals[k_prime_index].key = neighbor_records[1].key;

            neighbor_tuple.node->erase<record_t>(0);
        }
    }

    CHECK(commit_node(target_tuple));
    CHECK(commit_node(neighbor_tuple));
    CHECK(commit_node(parent_tuple));

    return true;
}

std::unique_ptr<record_t> BPTree::find(keyType key)
{
    auto leaf = find_leaf(key);
    if (!leaf.node)
    {
        return nullptr;
    }

    auto& header = leaf.node->nodePageHeader();
    auto& records = leaf.node->records();

    int i;
    for (i = 0; i < static_cast<int>(header.numberOfKeys); ++i)
    {
        if (records[i].key == key)
        {
            break;
        }
    }

    if (i == static_cast<int>(header.numberOfKeys))
    {
        return nullptr;
    }
    else
    {
        return std::make_unique<record_t>(records[i]);
    }
}

node_tuple BPTree::find_leaf(keyType key)
{
    auto root = manager.fileHeader.rootPageNumber;
    if (!is_valid(root))
    {
        if (verbose_output)
        {
            // 로그 출력
        }
        return node_tuple::invalid();
    }

    auto now = load_node(root);

    while (!now->nodePageHeader().isLeaf)
    {
        auto& header = now->nodePageHeader();
        auto& internal = now->internals();

        if (verbose_output)
        {
            std::cout << '[';
            for (auto& internal : now->range<internal_t>())
            {
                std::cout << internal.key << ' ';
            }
            std::cout << "] \n";
        }

        int idx = now->satisfy_condition_first<internal_t>([&](auto& now) {
            return now.key > key;
        }) - 1;

        if (verbose_output)
        {
            printf("%d ->\n", idx);
        }

        now = load_node(root = (idx == -1) ? header.onePageNumber
                                           : internal[idx].node_id);
    }

    if (verbose_output)
    {
        std::cout << "Leaf [";
        for (auto& record : now->range<record_t>())
        {
            std::cout << record.key << " ";
        }
        std::cout << "] ->\n";
    }

    return { root, std::move(now) };
}

std::unique_ptr<record_t> BPTree::make_record(keyType key,
                                              const valType& value) const
{
    auto rec = std::make_unique<record_t>();
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
    auto root = make_node(true);

    root->insert(rec, 0);

    auto pagenum = manager.pageCreate();
    CHECK(commit_node(pagenum, *root));
    manager.set_root(pagenum);

    return true;
}

bool BPTree::is_valid(nodeId_t node_id) const
{
    return node_id != INVALID_NODE_ID;
}