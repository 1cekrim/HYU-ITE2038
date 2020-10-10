#include "bpt.hpp"

#include <iostream>
#include <vector>

#include "logger.hpp"

BPTree::BPTree(FileManager& fm, bool verbose_output)
    : fm(fm),
      leaf_order(LEAF_ORDER),
      internal_order(INTERNAL_ORDER),
      verbose_output(verbose_output)
{
    // Do nothing
}

int BPTree::char_to_valType(valType& dst, const char* src) const
{
    int result;
    for (result = 0; result < value_size - 1 && *src; ++result, ++src)
    {
        dst[result] = *src;
    }

    for (int i = result; i < value_size - 1; ++i)
    {
        dst[i] = 0;
    }

    dst[value_size - 1] = 0;
    return result;
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
    auto& header = root->header.nodePageHeader;
    auto& internal = root->entry.internals;

    header.onePageNumber = left.pagenum;

    // TODO: insert_internal
    internal[0].key = key;
    internal[0].pageNumber = right.pagenum;
    ++header.numberOfKeys;

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

    page_t new_page;
    auto& new_header = new_page.header.nodePageHeader;
    auto& new_internals = new_page.entry.internals;

    new_header.onePageNumber = temp[split - 1].pageNumber;
    for (++i; i < internal_order; ++i)
    {
        new_page.push_internal(temp[i]);
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
    ASSERT_WITH_LOG(fm.pageWrite(new_pagenum, new_page), false,
                    "new page write failure");

    node_tuple new_tuple { std::move(new_node), new_pagenum };
    return insert_into_parent(parent, k_prime, new_tuple);
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

        fm.pageRead(root = (i == -1) ? header.onePageNumber : internal[i].pageNumber, *now);
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
    puts("start_new_tree");
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
