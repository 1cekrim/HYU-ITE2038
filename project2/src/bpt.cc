#include "bpt.hpp"

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

    auto pointer { make_record(key, value) };

    auto root = fm.fileHeader.rootPageNumber;
    if (root == EMPTY_PAGE_NUMBER)
    {
        return start_new_tree(*pointer);
    }

    auto nodePair = find_leaf(key);

    auto& header = nodePair.n->nodePageHeader();
    if (static_cast<int>(header.numberOfKeys) < leaf_order - 1)
    {
        return insert_into_leaf(nodePair, *pointer);
    }

    return insert_into_leaf_after_splitting(nodePair, *pointer);
}

bool BPTree::insert_into_leaf(node_tuple& leaf, const record& rec)
{
    int insertion_point =
        leaf.n->satisfy_condition_first<Record>([&](auto& now) {
            return now.key >= rec.key;
        });

    leaf.n->insert(rec, insertion_point);

    CHECK(commit_node(leaf));

    return true;
}

bool BPTree::insert_into_leaf_after_splitting(node_tuple& leaf_tuple,
                                              const record& rec)
{
    auto new_leaf = make_node(true);
    auto new_pagenum = fm.pageCreate();
    CHECK_WITH_LOG(new_pagenum != EMPTY_PAGE_NUMBER, false,
                   "page create failure");

    auto& leaf_node = leaf_tuple.n;
    auto& leaf_pagenum = leaf_tuple.pagenum;

    auto& header = leaf_node->nodePageHeader();

    int insertion_index =
        leaf_node->satisfy_condition_first<Record>([&](auto& now) {
            return now.key >= rec.key;
        });

    std::vector<record> temp;
    temp.reserve(leaf_order + 1);

    auto back = std::back_inserter(temp);
    leaf_node->range_copy<Record>(back, 0, insertion_index);
    back = rec;
    leaf_node->range_copy<Record>(back, insertion_index);

    int split = cut(leaf_order - 1);

    leaf_node->range_assignment<Record>(std::begin(temp),
                                        std::next(std::begin(temp), split));
    new_leaf->range_assignment<Record>(std::next(std::begin(temp), split),
                                       std::end(temp));

    auto& new_header = new_leaf->nodePageHeader();
    auto& new_records = new_leaf->records();
    new_header.onePageNumber = header.onePageNumber;
    header.onePageNumber = new_pagenum;
    new_header.parentPageNumber = header.parentPageNumber;

    CHECK(commit_node(leaf_pagenum, *leaf_node));
    CHECK(commit_node(new_pagenum, *new_leaf));

    struct node_tuple right = { new_pagenum, std::move(new_leaf) };
    return insert_into_parent(leaf_tuple, new_records[0].key, right);
}

bool BPTree::insert_into_parent(node_tuple& left, keyType key,
                                node_tuple& right)
{
    auto& left_header = left.n->nodePageHeader();

    auto parent_pagenum = left_header.parentPageNumber;

    if (parent_pagenum == EMPTY_PAGE_NUMBER)
    {
        return insert_into_new_root(left, key, right);
    }

    node_tuple parent { parent_pagenum, load_node(parent_pagenum) };
    CHECK(parent.n);

    int left_index = get_left_index(*parent.n, left.pagenum);
    if (static_cast<int>(parent.n->nodePageHeader().numberOfKeys) <
        internal_order - 1)
    {
        return insert_into_node(parent, left_index, key, right);
    }

    return insert_into_node_after_splitting(parent, left_index, key, right);
}

int BPTree::get_left_index(const node& parent, pagenum_t left_pagenum) const
{
    const auto& header = parent.nodePageHeader();
    if (header.onePageNumber == left_pagenum)
    {
        return 0;
    }

    int left_index = parent.satisfy_condition_first<Internal>([&](auto& now) {
        return now.pageNumber == left_pagenum;
    });
    return left_index + 1;
}

std::unique_ptr<node> BPTree::load_node(pagenum_t pagenum)
{
    auto node = make_node();
    CHECK_WITH_LOG(fm.pageRead(pagenum, *node), nullptr,
                   "read page failure: %ld", pagenum);
    return node;
}

bool BPTree::commit_node(pagenum_t page, const node& n)
{
    CHECK_WITH_LOG(fm.pageWrite(page, n), false, "write page failure: %ld",
                   page);
    return true;
}

bool BPTree::commit_node(const node_tuple& nt)
{
    CHECK_WITH_LOG(fm.pageWrite(nt.pagenum, *nt.n), false,
                   "write page failure: %ld", nt.pagenum);
    return true;
}

bool BPTree::insert_into_new_root(node_tuple& left_tuple, keyType key,
                                  node_tuple& right_tuple)
{
    auto root_pagenum = fm.pageCreate();
    CHECK_WITH_LOG(root_pagenum != EMPTY_PAGE_NUMBER, false,
                   "root page creation failure");

    auto root = make_node(false);
    auto& root_header = root->nodePageHeader();

    root_header.onePageNumber = left_tuple.pagenum;

    root->emplace_back<Internal>(key, right_tuple.pagenum);

    left_tuple.n->nodePageHeader().parentPageNumber = root_pagenum;
    right_tuple.n->nodePageHeader().parentPageNumber = root_pagenum;

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
    Internal internal { key, right.pagenum };
    parent.n->insert(internal, left_index);
    CHECK_WITH_LOG(fm.pageWrite(parent.pagenum, *parent.n), false,
                   "parent page write failure: %ld", parent.pagenum);
    return true;
}

bool BPTree::insert_into_node_after_splitting(node_tuple& parent,
                                              int left_index, keyType key,
                                              node_tuple& right)
{
    Internal internal { key, right.pagenum };

    auto& parent_header = parent.n->nodePageHeader();
    auto& parent_internal = parent.n->entry.internals;

    auto new_pagenum = fm.pageCreate();
    CHECK_WITH_LOG(new_pagenum != EMPTY_PAGE_NUMBER, false,
                   "new page creation failure");

    // std::vector<Internal> temp;
    // temp.reserve(internal_order);

    // auto& parent_node = parent.n;
    // auto back = std::back_inserter(temp);

    std::vector<Internal> temp(internal_order);

    for (int i = 0, j = 0; i < static_cast<int>(parent_header.numberOfKeys);
         ++i, ++j)
    {
        if (j == left_index)
        {
            ++j;
        }
        temp[j] = parent_internal[i];
    }
    temp[left_index] = internal;

    // parent_node->range_copy<Internal>(back, 0, left_index);
    // back = internal;
    // parent_node->range_copy<Internal>(back, left_index);

    int split = cut(internal_order);

    auto new_node = make_node(false);

    // parent의 onePageNumber는 변경되지 않음이 보장되니 고려하지 않음
    int i;
    parent_header.numberOfKeys = 0;
    for (i = 0; i < split - 1; ++i)
    {
        parent.n->push_back(temp[i]);
    }
    keyType k_prime = temp[split - 1].key;

    auto& new_header = new_node->nodePageHeader();

    new_header.onePageNumber = temp[split - 1].pageNumber;
    for (++i; i < internal_order; ++i)
    {
        new_node->push_back(temp[i]);
    }
    new_header.parentPageNumber = parent_header.parentPageNumber;

    CHECK_WITH_LOG(
        update_parent_with_commit(new_header.onePageNumber, new_pagenum), false,
        "update parent with commit failure");

    for (auto& tmp : new_node->range<Internal>())
    {
        CHECK_WITH_LOG(update_parent_with_commit(tmp.pageNumber, new_pagenum),
                       false, "update parent with commit failure: %ld",
                       tmp.pageNumber);
    }

    CHECK_WITH_LOG(fm.pageWrite(parent.pagenum, *parent.n), false,
                   "parent page write failure");
    CHECK_WITH_LOG(fm.pageWrite(new_pagenum, *new_node), false,
                   "new page write failure");

    node_tuple new_tuple { new_pagenum, std::move(new_node) };
    return insert_into_parent(parent, k_prime, new_tuple);
}

bool BPTree::delete_key(keyType key)
{
    auto record = find(key);
    auto leaf = find_leaf(key);
    CHECK_WITH_LOG(leaf.pagenum != EMPTY_PAGE_NUMBER, false,
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

    if (target_tuple.pagenum == fm.fileHeader.rootPageNumber)
    {
        return adjust_root();
    }

    auto& target_header = target_tuple.n->nodePageHeader();

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
                   target_header.parentPageNumber, target_tuple.pagenum);
    auto& parent_internals = parent->internals();

    int neighbor_index = get_left_index(*parent, target_tuple.pagenum) - 1;
    int k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
    keyType k_prime = parent_internals[k_prime_index].key;

    node_tuple neighbor_tuple;
    neighbor_tuple.pagenum =
        neighbor_index == -1
            ? parent_internals[0].pageNumber
            : neighbor_index == 0
                  ? parent_header.onePageNumber
                  : parent_internals[neighbor_index - 1].pageNumber;
    neighbor_tuple.n = load_node(neighbor_tuple.pagenum);
    CHECK(neighbor_tuple.n);

    int capacity = target_header.isLeaf ? leaf_order : internal_order - 1;

    node_tuple parent_tuple { target_header.parentPageNumber,
                              std::move(parent) };

    if (static_cast<int>(target_header.numberOfKeys +
                         neighbor_tuple.n->nodePageHeader().numberOfKeys) <
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
    auto& header = target.n->nodePageHeader();
    if (header.isLeaf)
    {
        int idx = target.n->satisfy_condition_first<Record>([&](auto& now) {
            return now.key == key;
        });
        CHECK_WITH_LOG(idx != static_cast<int>(header.numberOfKeys), false,
                       "invalid key: %ld", key);
        target.n->erase<Record>(idx);
    }
    else
    {
        int idx = target.n->satisfy_condition_first<Internal>([&](auto& now) {
            return now.key == key;
        });
        CHECK_WITH_LOG(idx != static_cast<int>(header.numberOfKeys), false,
                       "invalid key: %ld", key);
        target.n->erase<Internal>(idx);
    }

    return true;
}

bool BPTree::adjust_root()
{
    auto root = make_node(false);
    pagenum_t root_pagenum = fm.fileHeader.rootPageNumber;

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
        pagenum_t new_root_pagenum = root_header.onePageNumber;
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

bool BPTree::update_parent_with_commit(pagenum_t target_pagenum,
                                       pagenum_t parent_pagenum)
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
    auto& target_header = target_tuple.n->nodePageHeader();
    auto& neighbor_header = neighbor_tuple.n->nodePageHeader();

    if (target_header.isLeaf)
    {
        auto& neighbor = neighbor_tuple.n;
        for (auto& rec : target_tuple.n->range<Record>())
        {
            neighbor->push_back(rec);
        }
        neighbor_header.onePageNumber = target_header.onePageNumber;
    }
    else
    {
        // internal
        auto& neighbor = neighbor_tuple.n;

        pagenum_t pagenum;
        neighbor->emplace_back<Internal>(k_prime,
                                         pagenum = target_header.onePageNumber);
        CHECK(update_parent_with_commit(pagenum, neighbor_tuple.pagenum));
        for (auto& internal : target_tuple.n->range<Internal>())
        {
            pagenum = internal.pageNumber;
            neighbor->push_back(internal);

            CHECK(update_parent_with_commit(pagenum, neighbor_tuple.pagenum));
        }
    }

    CHECK(commit_node(neighbor_tuple));
    CHECK_WITH_LOG(delete_entry(parent_tuple, k_prime), false,
                   "delete entry of parent failure: %ld", parent_tuple.pagenum);
    CHECK_WITH_LOG(fm.pageFree(target_tuple.pagenum), false,
                   "free target page failure: %ld", target_tuple.pagenum);

    return true;
}

bool BPTree::redistribute_nodes(node_tuple& target_tuple,
                                node_tuple& neighbor_tuple,
                                node_tuple& parent_tuple, int k_prime,
                                int k_prime_index, int neighbor_index)
{
    auto& target_header = target_tuple.n->nodePageHeader();
    auto& neighbor_header = neighbor_tuple.n->nodePageHeader();
    auto& parent_internals = parent_tuple.n->internals();

    if (neighbor_index != -1)
    {
        // left neighbor
        if (!target_header.isLeaf)
        {
            auto& neighbor_internals = neighbor_tuple.n->internals();

            target_tuple.n->insert<Internal>(
                { k_prime, target_header.onePageNumber }, 0);

            Internal& new_one =
                neighbor_internals[neighbor_header.numberOfKeys - 1];
            parent_internals[k_prime_index].key = new_one.key;
            target_header.onePageNumber = new_one.pageNumber;

            CHECK_WITH_LOG(update_parent_with_commit(new_one.pageNumber,
                                                     target_tuple.pagenum),
                           false, "update parent with commit failure");
        }
        else
        {
            auto& neighbor_records = neighbor_tuple.n->records();
            auto& target_records = target_tuple.n->records();

            target_tuple.n->insert(
                neighbor_records[neighbor_header.numberOfKeys - 1], 0);

            parent_internals[k_prime_index].key = target_records[0].key;
        }
    }
    else
    {
        // right neighbor
        if (!target_header.isLeaf)
        {
            auto& neighbor_internals = neighbor_tuple.n->internals();
            auto& target_internals = target_tuple.n->internals();

            target_tuple.n->emplace_back<Internal>(
                k_prime, neighbor_header.onePageNumber);

            parent_internals[k_prime_index].key = neighbor_internals[0].key;
            neighbor_header.onePageNumber = neighbor_internals[0].pageNumber;

            CHECK(update_parent_with_commit(
                target_internals[target_header.numberOfKeys - 1].pageNumber,
                target_tuple.pagenum));

            neighbor_tuple.n->erase<Internal>(0);
        }
        else
        {
            auto& neighbor_records = neighbor_tuple.n->records();

            target_tuple.n->push_back(neighbor_records[0]);

            parent_internals[k_prime_index].key = neighbor_records[1].key;

            neighbor_tuple.n->erase<Record>(0);
        }
    }

    CHECK(commit_node(target_tuple));
    CHECK(commit_node(neighbor_tuple));
    CHECK(commit_node(parent_tuple));

    return true;
}

std::unique_ptr<record> BPTree::find(keyType key)
{
    auto leaf = find_leaf(key);
    if (!leaf.n)
    {
        return nullptr;
    }

    auto& header = leaf.n->nodePageHeader();
    auto& records = leaf.n->records();

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
        return std::make_unique<record>(records[i]);
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
            for (auto& internal : now->range<Internal>())
            {
                std::cout << internal.key << ' ';
            }
            std::cout << "] \n";
        }

        int idx = now->satisfy_condition_first<Internal>([&](auto& now) {
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
        for (auto& record : now->range<Record>())
        {
            std::cout << record.key << " ";
        }
        std::cout << "] ->\n";
    }

    return { root, std::move(now) };
}

std::unique_ptr<record> BPTree::make_record(keyType key,
                                            const valType& value) const
{
    auto rec = std::make_unique<record>();
    rec->init(key, value);

    return rec;
}

std::unique_ptr<node> BPTree::make_node(bool is_leaf) const
{
    auto n = std::make_unique<node>();
    CHECK_WITH_LOG(n, nullptr, "allocation failure: node");

    auto& header = n->nodePageHeader();

    header.isLeaf = is_leaf;

    return n;
}

bool BPTree::start_new_tree(const record& rec)
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
