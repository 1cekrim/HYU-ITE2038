#include "bpt.hpp"

#include <cstring>
#include <iostream>
#include <vector>

#include "logger.hpp"

BPTree::BPTree(int delayed_min, bool verbose_output)
    : leaf_order(LEAF_ORDER),
      internal_order(INTERNAL_ORDER),
      verbose_output(verbose_output),
      delayed_min(delayed_min)
{
    // Do nothing
}

bool BPTree::open_table(const std::string& filename)
{
    ASSERT_WITH_LOG(fm.open(filename), false, "open table failure");
    return true;
}

int BPTree::char_to_valType(valType& dst, const char* src) const
{
    memset(&dst, 0, sizeof(&dst));
    memcpy(&dst, src, std::max(static_cast<int>(strlen(src)), 119));
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

    // key와 value에 대한 새로운 record 생성
    auto pointer { make_record(key, value) };

    // case1. root가 비어있으면 start_new_tree 함수를 호출하고 리턴
    auto root = fm.fileHeader.rootPageNumber;
    if (root == EMPTY_PAGE_NUMBER)
    {
        return start_new_tree(*pointer);
    }

    auto nodePair = find_leaf(key);
    auto& [leaf, pagenum] = nodePair;

    auto& header = leaf->header.nodePageHeader;
    auto& records = leaf->entry.records;
    if (header.numberOfKeys < leaf_order - 1)
    {
        return insert_into_leaf(nodePair, *pointer);
    }

    return insert_into_leaf_after_splitting(nodePair, *pointer);
}

bool BPTree::insert_into_leaf(node_tuple& leaf, const record& rec)
{
    int insertion_point {};

    auto& [c, pagenum] = leaf;

    auto& header = c->header.nodePageHeader;
    auto& records = c->entry.records;

    while (insertion_point < header.numberOfKeys &&
           records[insertion_point].key < rec.key)
    {
        ++insertion_point;
    }

    c->insert_record(rec, insertion_point);
    ASSERT_WITH_LOG(fm.pageWrite(pagenum, *c), false, "page write failure");

    return true;
}

bool BPTree::insert_into_leaf_after_splitting(node_tuple& leaf_tuple,
                                              const record& rec)
{
    auto new_leaf = make_node(true);
    auto new_pagenum = fm.pageCreate();
    ASSERT_WITH_LOG(new_pagenum != EMPTY_PAGE_NUMBER, false,
                    "page create failure");

    auto& leaf = leaf_tuple.n;
    auto pagenum = leaf_tuple.pagenum;

    auto& header = leaf->header.nodePageHeader;
    auto& records = leaf->entry.records;

    int insertion_index = 0;
    while (insertion_index < header.numberOfKeys &&
           records[insertion_index].key < rec.key)
    {
        ++insertion_index;
    }

    std::vector<record> temp(leaf_order + 1);

    for (int i = 0, j = 0; i < header.numberOfKeys; ++i, ++j)
    {
        if (j == insertion_index)
        {
            ++j;
        }
        temp[j] = records[i];
    }

    temp[insertion_index] = rec;

    header.numberOfKeys = 0;

    int split = cut(leaf_order - 1);

    for (int i = 0; i < split; ++i)
    {
        records[i] = temp[i];
        ++header.numberOfKeys;
    }

    auto& new_header = new_leaf->header.nodePageHeader;
    auto& new_records = new_leaf->entry.records;

    for (int i = split, j = 0; i < leaf_order; ++i, ++j)
    {
        new_records[j] = temp[i];
        ++new_header.numberOfKeys;
    }

    new_header.onePageNumber = header.onePageNumber;
    header.onePageNumber = new_pagenum;
    new_header.parentPageNumber = header.parentPageNumber;

    ASSERT_WITH_LOG(fm.pageWrite(pagenum, *leaf), false,
                    "leaf page write failure");
    ASSERT_WITH_LOG(fm.pageWrite(new_pagenum, *new_leaf), false,
                    "new leaf page write failure");

    struct node_tuple right = { std::move(new_leaf), new_pagenum };
    return insert_into_parent(leaf_tuple, new_records[0].key, right);
}

bool BPTree::insert_into_parent(node_tuple& left, keyType key,
                                node_tuple& right)
{
    auto left_pagenum = left.pagenum;
    auto& left_header = left.n->header.nodePageHeader;
    auto& left_record = left.n->entry.records;

    auto right_pagenum = right.pagenum;
    auto& right_header = right.n->header.nodePageHeader;
    auto& right_record = right.n->entry.records;

    auto parent_pagenum = left_header.parentPageNumber;

    if (parent_pagenum == EMPTY_PAGE_NUMBER)
    {
        return insert_into_new_root(left, key, right);
    }

    node_tuple parent;
    parent.pagenum = parent_pagenum;
    parent.n = std::make_unique<node>();

    ASSERT_WITH_LOG(fm.pageRead(parent_pagenum, *parent.n), false,
                    "read parent page failure: %ld", parent_pagenum);

    int left_index = get_left_index(*parent.n, left.pagenum);
    if (parent.n->header.nodePageHeader.numberOfKeys < internal_order - 1)
    {
        return insert_into_node(parent, left_index, key, right);
    }

    return insert_into_node_after_splitting(parent, left_index, key, right);
}

int BPTree::get_left_index(const node& parent, pagenum_t left_pagenum) const
{
    int left_index = 0;
    auto& header = parent.header.nodePageHeader;
    auto& internal = parent.entry.internals;
    if (header.onePageNumber == left_pagenum)
    {
        return 0;
    }

    while (left_index < header.numberOfKeys &&
           internal[left_index].pageNumber != left_pagenum)
    {
        ++left_index;
    }
    return left_index + 1;
}

bool BPTree::insert_into_new_root(node_tuple& left, keyType key,
                                  node_tuple& right)
{
    auto root_pagenum = fm.pageCreate();
    ASSERT_WITH_LOG(root_pagenum != EMPTY_PAGE_NUMBER, false,
                    "root page creation failure");

    auto root = make_node(false);
    auto& root_header = root->header.nodePageHeader;
    auto& root_internal = root->entry.internals;

    root_header.onePageNumber = left.pagenum;

    // TODO: insert_internal
    root_internal[0].key = key;
    root_internal[0].pageNumber = right.pagenum;
    ++root_header.numberOfKeys;

    left.n->header.nodePageHeader.parentPageNumber = root_pagenum;
    right.n->header.nodePageHeader.parentPageNumber = root_pagenum;

    ASSERT_WITH_LOG(fm.pageWrite(left.pagenum, *left.n), false,
                    "left page write failure: %ld", left.pagenum);
    ASSERT_WITH_LOG(fm.pageWrite(right.pagenum, *right.n), false,
                    "right page write failure: %ld", right.pagenum);
    ASSERT_WITH_LOG(fm.pageWrite(root_pagenum, *root), false,
                    "root page write failure: %ld", root_pagenum);

    fm.fileHeader.rootPageNumber = root_pagenum;
    ASSERT_WITH_LOG(fm.updateFileHeader(), false, "update file header failure");

    return true;
}

bool BPTree::insert_into_node(node_tuple& parent, int left_index, keyType key,
                              node_tuple& right)
{
    Internal internal;
    internal.init(key, right.pagenum);
    parent.n->insert_internal(internal, left_index);
    ASSERT_WITH_LOG(fm.pageWrite(parent.pagenum, *parent.n), false,
                    "parent page write failure: %ld", parent.pagenum);
    return true;
}

bool BPTree::insert_into_node_after_splitting(node_tuple& parent,
                                              int left_index, keyType key,
                                              node_tuple& right)
{
    Internal internal;
    internal.init(key, right.pagenum);

    auto& parent_header = parent.n->header.nodePageHeader;
    auto& parent_internal = parent.n->entry.internals;

    auto new_pagenum = fm.pageCreate();
    ASSERT_WITH_LOG(new_pagenum != EMPTY_PAGE_NUMBER, false,
                    "new page creation failure");

    std::vector<Internal> temp(internal_order);

    for (int i = 0, j = 0; i < parent_header.numberOfKeys; ++i, ++j)
    {
        if (j == left_index)
        {
            ++j;
        }
        temp[j] = parent_internal[i];
    }
    temp[left_index] = internal;

    int split = cut(internal_order);

    auto new_node = make_node(false);

    // parent의 onePageNumber는 변경되지 않음이 보장되니 고려하지 않음
    int i, j;
    parent_header.numberOfKeys = 0;
    for (i = 0; i < split - 1; ++i)
    {
        parent.n->push_internal(temp[i]);
    }
    keyType k_prime = temp[split - 1].key;

    auto& new_header = new_node->header.nodePageHeader;
    auto& new_internals = new_node->entry.internals;

    new_header.onePageNumber = temp[split - 1].pageNumber;
    for (++i; i < internal_order; ++i)
    {
        new_node->push_internal(temp[i]);
    }
    new_header.parentPageNumber = parent_header.parentPageNumber;

    auto change_parent = [&](pagenum_t pagenum) {
        page_t page;
        ASSERT_WITH_LOG(fm.pageRead(pagenum, page), false,
                        "page read failure: %ld", pagenum);
        page.header.nodePageHeader.parentPageNumber = new_pagenum;
        ASSERT_WITH_LOG(fm.pageWrite(pagenum, page), false,
                        "page write failure: %ld", pagenum);
        return true;
    };

    ASSERT_WITH_LOG(change_parent(new_header.onePageNumber), false,
                    "change parent of onePage failure");
    for (int i = 0; i < new_header.numberOfKeys; ++i)
    {
        ASSERT_WITH_LOG(change_parent(new_internals[i].pageNumber), false,
                        "change parent of %d failure", i);
    }

    ASSERT_WITH_LOG(fm.pageWrite(parent.pagenum, *parent.n), false,
                    "parent page write failure");
    ASSERT_WITH_LOG(fm.pageWrite(new_pagenum, *new_node), false,
                    "new page write failure");

    node_tuple new_tuple { std::move(new_node), new_pagenum };
    return insert_into_parent(parent, k_prime, new_tuple);
}

bool BPTree::delete_key(keyType key)
{
    auto record = find(key);
    auto leaf = find_leaf(key);
    ASSERT_WITH_LOG(leaf.pagenum != EMPTY_PAGE_NUMBER, false,
                    "find leaf failure: %ld", key);
    if (!record)
    {
        return false;
    }

    ASSERT_WITH_LOG(delete_entry(leaf, key), false, "delete entry failure");

    return true;
}

bool BPTree::delete_entry(node_tuple& target, keyType key)
{
    ASSERT_WITH_LOG(remove_entry_from_node(target, key), false,
                    "remove entry from node failure: %ld", key);
    ASSERT_WITH_LOG(fm.pageWrite(target.pagenum, *target.n), false,
                    "write target page failure: %ld", target.pagenum);

    if (target.pagenum == fm.fileHeader.rootPageNumber)
    {
        return adjust_root();
    }

    auto& target_header = target.n->header.nodePageHeader;

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

    if (target_header.numberOfKeys >= min_keys)
    {
        return true;
    }

    auto parent = make_node(false);
    ASSERT_WITH_LOG(fm.pageRead(target_header.parentPageNumber, *parent), false,
                    "read parent page failure: %ld",
                    target_header.parentPageNumber);

    auto& parent_header = parent->header.nodePageHeader;
    auto& parent_internals = parent->entry.internals;

    int neighbor_index = get_left_index(*parent, target.pagenum);
    int k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
    keyType k_prime = parent_internals[k_prime_index].key;

    node_tuple neighbor_tuple;
    neighbor_tuple.n = make_node(false);
    neighbor_tuple.pagenum =
        neighbor_index == -1
            ? parent_internals[0].pageNumber
            : neighbor_index == 0
                  ? parent_header.onePageNumber
                  : parent_internals[neighbor_index - 1].pageNumber;
    ASSERT_WITH_LOG(fm.pageRead(neighbor_tuple.pagenum, *neighbor_tuple.n),
                    false, "read neighbor_tuple page failure: %ld",
                    neighbor_tuple.pagenum);

    int capacity = target_header.isLeaf ? leaf_order : internal_order - 1;

    node_tuple parent_tuple { std::move(parent),
                              target_header.parentPageNumber };

    node_tuple tmp_neightbor, tmp_target;
    if (neighbor_index == -1)
    {
        tmp_neightbor = std::move(target);
        tmp_target = std::move(neighbor_tuple);
    }
    else
    {
        tmp_neightbor = std::move(neighbor_tuple);
        tmp_target = std::move(target);
    }

    if (target_header.numberOfKeys +
            neighbor_tuple.n->header.nodePageHeader.numberOfKeys <
        capacity)
    {
        return coalesce_nodes(tmp_target, tmp_neightbor, parent_tuple, k_prime);
    }
    else
    {
        return redistribute_nodes(tmp_target, tmp_neightbor, parent_tuple, k_prime,
                                  k_prime_index);
    }
}

bool BPTree::remove_entry_from_node(node_tuple& target, keyType key)
{
    auto& header = target.n->header.nodePageHeader;
    if (header.isLeaf)
    {
        auto& records = target.n->entry.records;
        int i = 0;
        while (i < header.numberOfKeys && records[i].key != key)
        {
            ++i;
        }
        ASSERT_WITH_LOG(i != header.numberOfKeys, false, "invalid key: %ld",
                        key);
        for (++i; i < header.numberOfKeys; ++i)
        {
            records[i - 1] = records[i];
        }
        --header.numberOfKeys;
    }
    else
    {
        auto& internals = target.n->entry.internals;
        int i = 0;
        while (i < header.numberOfKeys && internals[i].key != key)
        {
            ++i;
        }
        ASSERT_WITH_LOG(i != header.numberOfKeys, false, "invalid key: %ld",
                        key);
        for (++i; i < header.numberOfKeys; ++i)
        {
            internals[i - 1] = internals[i];
        }
        --header.numberOfKeys;
    }
}

bool BPTree::adjust_root()
{
    page_t root;
    pagenum_t root_pagenum = fm.fileHeader.rootPageNumber;

    ASSERT_WITH_LOG(fm.pageRead(root_pagenum, root), false, "read root page failure: %ld", root_pagenum);

    auto& root_header = root.header.nodePageHeader;

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

        page_t new_root;
        ASSERT_WITH_LOG(fm.pageRead(new_root_pagenum, new_root), false, "read new root failure: %ld", new_root_pagenum);
        new_root.header.nodePageHeader.parentPageNumber = EMPTY_PAGE_NUMBER;
        ASSERT_WITH_LOG(fm.pageWrite(new_root_pagenum, new_root), false, "write new root failure: %ld", new_root_pagenum);
    }

    ASSERT_WITH_LOG(fm.updateFileHeader(), false, "update file header failure");
    ASSERT_WITH_LOG(fm.pageFree(root_pagenum), false, "free old root page failure");

    return true;
}

bool BPTree::coalesce_nodes(node_tuple& target_tuple,
                            node_tuple& neighbor_tuple,
                            node_tuple& parent_tuple, int k_prime)
{
    auto& target_header = target_tuple.n->header.nodePageHeader;
    auto& neighbor_header = neighbor_tuple.n->header.nodePageHeader;
    auto& parent_header = parent_tuple.n->header.nodePageHeader;

    int neighbor_insertion_index = neighbor_header.numberOfKeys;

    if (target_header.isLeaf)
    {
        auto& target_records = target_tuple.n->entry.records;
        auto& neighbor = neighbor_tuple.n;
        for (int i = 0; i < target_header.numberOfKeys; ++i)
        {
            // TODO: merge record
            neighbor->push_record(target_records[i]);
        }
        neighbor_header.onePageNumber = target_header.onePageNumber;
    }
    else
    {
        // internal
        auto& target_internals = target_tuple.n->entry.internals;
        auto& neighbor = neighbor_tuple.n;

        page_t temp;

        for (int i = -1; i < target_header.numberOfKeys; ++i)
        {
            int pagenum;
            if (i == -1)
            {
                neighbor->push_internal({k_prime, pagenum = target_header.onePageNumber});
            }
            else
            {
                neighbor->push_internal(target_internals[i]);
                pagenum = target_internals[i].pageNumber;
            }

            ASSERT_WITH_LOG(fm.pageRead(pagenum, temp), false, "read child page failure: %ld", pagenum);
            temp.header.nodePageHeader.parentPageNumber = neighbor_tuple.pagenum;
            ASSERT_WITH_LOG(fm.pageWrite(pagenum, temp), false, "write child page failure: %ld", pagenum);
        }
    }

    ASSERT_WITH_LOG(fm.pageWrite(neighbor_tuple.pagenum, *neighbor_tuple.n), false, "write neighbor page failure: %ld", neighbor_tuple.pagenum);
    ASSERT_WITH_LOG(delete_entry(parent_tuple, k_prime), false, "delete entry of parent failure: %ld", parent_tuple.pagenum);
    ASSERT_WITH_LOG(fm.pageFree(target_tuple.pagenum), false, "free target page failure: %ld", target_tuple.pagenum);

    return true;
}

bool BPTree::redistribute_nodes(node_tuple& target_tuple,
                                node_tuple& neighbor_tuple,
                                node_tuple& parent_tuple, int k_prime,
                                int k_prime_index)
{
}

std::unique_ptr<record> BPTree::find(keyType key)
{
    auto [c, pagenum] = find_leaf(key);
    if (!c)
    {
        return nullptr;
    }

    auto& header = c->header.nodePageHeader;
    auto& records = c->entry.records;

    int i;
    for (i = 0; i < header.numberOfKeys; ++i)
    {
        if (records[i].key == key)
        {
            break;
        }
    }

    if (i == header.numberOfKeys)
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

    auto now = std::make_unique<node>();
    ASSERT_WITH_LOG(fm.pageRead(root, *now), node_tuple::invalid(),
                    "page read failure");

    while (!now->header.nodePageHeader.isLeaf)
    {
        int i;
        auto& header = now->header.nodePageHeader;
        auto& internal = now->entry.internals;

        if (verbose_output)
        {
            printf("[");
            for (i = 0; i < header.numberOfKeys - 1; ++i)
            {
                printf("%ld ", internal[i].key);
            }
            printf("%ld] ", internal[i].key);
        }

        i = 0;
        while (i < header.numberOfKeys)
        {
            if (key >= internal[i].key)
            {
                ++i;
            }
            else
            {
                break;
            }
        }
        --i;

        if (verbose_output)
        {
            printf("%d ->\n", i);
        }

        fm.pageRead(
            root = ((i == -1) ? header.onePageNumber : internal[i].pageNumber),
            *now);
    }

    if (verbose_output)
    {
        auto& header = now->header.nodePageHeader;
        auto& records = now->entry.records;
        printf("Leaf [");
        int i;
        for (i = 0; i < header.numberOfKeys - 1; ++i)
        {
            printf("%ld ", records[i].key);
        }
        printf("%ld] ->\n", records[i].key);
    }

    return { std::move(now), root };
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
    ASSERT_WITH_LOG(n, nullptr, "allocation failure: node");

    auto& header = n->header.nodePageHeader;

    // 어차피 0으로 초기화 되어있는데 필요한가?
    header.isLeaf = is_leaf;
    header.numberOfKeys = 0;
    header.onePageNumber = EMPTY_PAGE_NUMBER;
    header.parentPageNumber = EMPTY_PAGE_NUMBER;

    return n;
}

bool BPTree::start_new_tree(const record& rec)
{
    auto root = make_node(true);

    root->insert_record(rec, 0);

    auto pagenum = fm.pageCreate();
    ASSERT_WITH_LOG(pagenum != EMPTY_PAGE_NUMBER, false,
                    "page creation failure");
    ASSERT_WITH_LOG(fm.pageWrite(pagenum, *root), false, "page write failure");

    fm.fileHeader.rootPageNumber = pagenum;
    ASSERT_WITH_LOG(fm.updateFileHeader(), false, "file header update failure");

    return true;
}
