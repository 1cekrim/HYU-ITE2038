#include "bpt.hpp"

#include <iostream>

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
    if (header.nubmerOfKeys < leaf_order - 1)
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

    while (insertion_point < header.nubmerOfKeys &&
           records[insertion_point].key < rec.key)
    {
        ++insertion_point;
    } 

    c->insert_record(rec, insertion_point);
    ASSERT_WITH_LOG(fm.pageWrite(pagenum, *c), false, "page write failure");

    return true;
}

bool BPTree::insert_into_leaf_after_splitting(node_tuple& leaf,
                                              const record& rec)
{
    // TODO: 구현
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
    for (i = 0; i < header.nubmerOfKeys; ++i)
    {
        if (records[i].key == key)
        {
            break;
        }
    }

    if (i == header.nubmerOfKeys)
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
            for (i = 0; i < header.nubmerOfKeys - 1; ++i)
            {
                printf("%ld ", internal[i].key);
            }
            printf("%ld] ", internal[i].key);
        }

        i = 0;
        while (i < header.nubmerOfKeys)
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

        if (verbose_output)
        {
            printf("%d ->\n", i);
        }

        fm.pageRead(root = internal[i].pageNumber, *now);
    }

    if (verbose_output)
    {
        auto& header = now->header.nodePageHeader;
        auto& records = now->entry.records;
        printf("Leaf [");
        int i;
        for (i = 0; i < header.nubmerOfKeys - 1; ++i)
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
    header.nubmerOfKeys = 0;
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
