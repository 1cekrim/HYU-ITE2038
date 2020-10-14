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
    CHECK_WITH_LOG(fm.open(filename), false, "open table failure");
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
    return fm.getFileDescriptor();
}

bool BPTree::insert(keyType key, const valType& value)
{
    // 중복을 허용하지 않음
    if (find(key))
    {
        return false;
    }

    auto record = make_record(key, value);

    if (fm.fileHeader.rootPageNumber == EMPTY_PAGE_NUMBER)
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
    int insertion_point = leaf.node->satisfy_condition_first<record_t>(
        [&](auto& now) { return now.key >= rec.key; });

    leaf.node->insert(rec, insertion_point);

    CHECK(commit_node(leaf));

    return true;
}

bool BPTree::insert_into_leaf_after_splitting(node_tuple& leaf,
                                              const record_t& rec)
{
    auto new_leaf = node_tuple{ create_node(), make_node(true) };

    int insertion_index = leaf.node->satisfy_condition_first<record_t>(
        [&](auto& now) { return now.key >= rec.key; });

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
    auto& left_header = left.node->nodePageHeader();

    auto parent_pagenum = left_header.parentPageNumber;

    if (parent_pagenum == EMPTY_PAGE_NUMBER)
    {
        return insert_into_new_root(left, key, right);
    }

    node_tuple parent{ parent_pagenum, load_node(parent_pagenum) };
    CHECK(parent.node);

    int left_index = get_left_index(*parent.node, left.id);
    if (static_cast<int>(parent.node->nodePageHeader().numberOfKeys) <
        internal_order - 1)
    {
        return insert_into_node(parent, left_index, key, right);
    }

    return insert_into_node_after_splitting(parent, left_index, key, right);
}

int BPTree::get_left_index(const node_t& parent, nodeId_t left_pagenum) const
{
    const auto& header = parent.nodePageHeader();
    if (header.onePageNumber == left_pagenum)
    {
        return 0;
    }

    int left_index = parent.satisfy_condition_first<internal_t>(
        [&](auto& now) { return now.pageNumber == left_pagenum; });
    return left_index + 1;
}

std::unique_ptr<node_t> BPTree::load_node(nodeId_t node_id)
{
    auto node = make_node();
    node->load(fm, node_id);
    return node;
}

bool BPTree::commit_node(nodeId_t node_id, const node_t& node)
{
    CHECK(node.commit(fm, node_id));
    return true;
}

bool BPTree::commit_node(const node_tuple& nt)
{
    CHECK(nt.node->commit(fm, nt.id));
    return true;
}

nodeId_t BPTree::create_node()
{
    nodeId_t t = fm.pageCreate();
    EXIT_WITH_LOG(t != EMPTY_PAGE_NUMBER, "create page failure");
    return t;
}

bool BPTree::insert_into_new_root(node_tuple& left_tuple, keyType key,
                                  node_tuple& right_tuple)
{
    auto root_pagenum = fm.pageCreate();
    CHECK_WITH_LOG(root_pagenum != EMPTY_PAGE_NUMBER, false,
                   "root page creation failure");

    auto root = make_node(false);
    auto& root_header = root->nodePageHeader();

    root_header.onePageNumber = left_tuple.id;

    root->emplace_back<internal_t>(key, right_tuple.id);

    left_tuple.node->nodePageHeader().parentPageNumber = root_pagenum;
    right_tuple.node->nodePageHeader().parentPageNumber = root_pagenum;

    CHECK(commit_node(left_tuple));
    CHECK(commit_node(right_tuple));
    CHECK(commit_node(root_pagenum, *root));

    fm.fileHeader.rootPageNumber = root_pagenum;
    CHECK_WITH_LOG(fm.updateFileHeader(), false, "update file header failure");

    return true;
}

bool BPTree::insert_into_node(node_tuple& parent, int left_index, keyType key,
                              node_tuple& right)
{
    Internal internal{ key, right.id };
    parent.node->insert(internal, left_index);
    CHECK_WITH_LOG(fm.pageWrite(parent.id, *parent.node), false,
                   "parent page write failure: %ld", parent.id);
    return true;
}

bool BPTree::insert_into_node_after_splitting(node_tuple& parent,
                                              int left_index, keyType key,
                                              node_tuple& right)
{
    Internal internal{ key, right.id };

    auto& parent_header = parent.node->nodePageHeader();

    auto new_pagenum = create_node();
    CHECK_WITH_LOG(new_pagenum != EMPTY_PAGE_NUMBER, false,
                   "new page creation failure");

    std::vector<internal_t> temp;
    temp.reserve(internal_order);

    auto& parent_node = parent.node;
    auto back = std::back_inserter(temp);
    parent_node->range_copy<internal_t>(back, 0, left_index);
    back = internal;
    parent_node->range_copy<internal_t>(back, left_index);

    int split = cut(internal_order);

    auto new_node = make_node(false);

    keyType k_prime = temp[split - 1].key;

    parent_node->range_assignment<internal_t>(
        std::begin(temp), std::next(std::begin(temp), split - 1));

    auto& new_header = new_node->nodePageHeader();

    new_header.onePageNumber = temp[split - 1].pageNumber;
    new_node->range_assignment<internal_t>(std::next(std::begin(temp), split),
                                           std::end(temp));
    new_header.parentPageNumber = parent_header.parentPageNumber;

    CHECK_WITH_LOG(
        update_parent_with_commit(new_header.onePageNumber, new_pagenum), false,
        "update parent with commit failure");

    for (auto& tmp : new_node->range<internal_t>())
    {
        CHECK_WITH_LOG(update_parent_with_commit(tmp.pageNumber, new_pagenum),
                       false, "update parent with commit failure: %ld",
                       tmp.pageNumber);
    }

    CHECK(commit_node(parent));
    CHECK(commit_node(new_pagenum, *new_node));

    CHECK_WITH_LOG(fm.pageWrite(parent.id, *parent.node), false,
                   "parent page write failure");
    CHECK_WITH_LOG(fm.pageWrite(new_pagenum, *new_node), false,
                   "new page write failure");

    node_tuple new_tuple{ new_pagenum, std::move(new_node) };
    return insert_into_parent(parent, k_prime, new_tuple);
}

bool BPTree::delete_key(keyType key)
{
    auto record = find(key);
    auto leaf = find_leaf(key);
    CHECK_WITH_LOG(leaf.id != EMPTY_PAGE_NUMBER, false,
                   "find leaf failure: %ld", key);
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
    CHECK(commit_node(target_tuple));

    if (target_tuple.id == fm.fileHeader.rootPageNumber)
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
            ? parent_internals[0].pageNumber
            : neighbor_index == 0
                  ? parent_header.onePageNumber
                  : parent_internals[neighbor_index - 1].pageNumber;
    neighbor_tuple.node = load_node(neighbor_tuple.id);
    CHECK(neighbor_tuple.node);

    int capacity = target_header.isLeaf ? leaf_order : internal_order - 1;

    node_tuple parent_tuple{ target_header.parentPageNumber,
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
        int idx = target.node->satisfy_condition_first<record_t>(
            [&](auto& now) { return now.key == key; });
        CHECK_WITH_LOG(idx != static_cast<int>(header.numberOfKeys), false,
                       "invalid key: %ld", key);
        target.node->erase<record_t>(idx);
    }
    else
    {
        int idx = target.node->satisfy_condition_first<internal_t>(
            [&](auto& now) { return now.key == key; });
        CHECK_WITH_LOG(idx != static_cast<int>(header.numberOfKeys), false,
                       "invalid key: %ld", key);
        target.node->erase<internal_t>(idx);
    }

    return true;
}

bool BPTree::adjust_root()
{
    auto root = make_node(false);
    nodeId_t root_pagenum = fm.fileHeader.rootPageNumber;

    CHECK_WITH_LOG(fm.pageRead(root_pagenum, *root), false,
                   "read root page failure: %ld", root_pagenum);

    auto& root_header = root->nodePageHeader();

    if (root_header.numberOfKeys > 0)
    {
        return true;
    }

    if (root_header.isLeaf)
    {
        fm.fileHeader.rootPageNumber = EMPTY_PAGE_NUMBER;
    }
    else
    {
        nodeId_t new_root_pagenum = root_header.onePageNumber;
        fm.fileHeader.rootPageNumber = new_root_pagenum;

        CHECK_WITH_LOG(
            update_parent_with_commit(new_root_pagenum, EMPTY_PAGE_NUMBER),
            false, "update parent with commit failure");
    }

    CHECK_WITH_LOG(fm.updateFileHeader(), false, "update file header failure");
    CHECK_WITH_LOG(fm.pageFree(root_pagenum), false,
                   "free old root page failure");

    return true;
}

bool BPTree::update_parent_with_commit(nodeId_t target_pagenum,
                                       nodeId_t parent_pagenum)
{
    auto temp = load_node(target_pagenum);
    CHECK(temp);
    temp->nodePageHeader().parentPageNumber = parent_pagenum;
    CHECK(commit_node(target_pagenum, *temp));

    return true;
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
            pagenum = internal.pageNumber;
            neighbor->push_back(internal);

            CHECK(update_parent_with_commit(pagenum, neighbor_tuple.id));
        }
    }

    CHECK(commit_node(neighbor_tuple));
    CHECK_WITH_LOG(delete_entry(parent_tuple, k_prime), false,
                   "delete entry of parent failure: %ld", parent_tuple.id);
    CHECK_WITH_LOG(fm.pageFree(target_tuple.id), false,
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
            target_header.onePageNumber = new_one.pageNumber;

            CHECK_WITH_LOG(
                update_parent_with_commit(new_one.pageNumber, target_tuple.id),
                false, "update parent with commit failure");
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
            neighbor_header.onePageNumber = neighbor_internals[0].pageNumber;

            CHECK(update_parent_with_commit(
                target_internals[target_header.numberOfKeys - 1].pageNumber,
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
    auto root = fm.fileHeader.rootPageNumber;
    if (root == EMPTY_PAGE_NUMBER)
    {
        if (verbose_output)
        {
            // 로그 출력
        }
        return node_tuple::invalid();
    }

    auto now = load_node(root);
    CHECK_WITH_LOG(now, node_tuple::invalid(), "page read failure");

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
                                           : internal[idx].pageNumber);
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

    auto pagenum = fm.pageCreate();
    CHECK_WITH_LOG(pagenum != EMPTY_PAGE_NUMBER, false,
                   "page creation failure");
    CHECK(commit_node(pagenum, *root));

    fm.fileHeader.rootPageNumber = pagenum;
    CHECK(fm.updateFileHeader());

    return true;
}
