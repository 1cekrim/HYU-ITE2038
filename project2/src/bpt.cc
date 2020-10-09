#include "bpt.hpp"

#include <iostream>

#include "logger.hpp"

BPTree::BPTree(FileManager& fm)
    : fm(fm),
      leaf_order(LEAF_ORDER),
      internal_order(INTERNAL_ORDER),
      verbose_output(VERBOSE_OUTPUT)
{
    // Do nothing
}

bool BPTree::insert(keyType key, const val_t& value)
{
    // 중복을 허용하지 않음
    if (find(key))
    {
        return false;
    }

    // key와 value에 대한 새로운 record 생성
    std::unique_ptr<record> pointer(make_record(key, value));

    // root가 비어있으면 start_new_tree 함수를 호출하고 리턴
    auto root = fm.fileHeader.rootPageNumber;
    if (root == EMPTY_PAGE_NUMBER)
    {
        return start_new_tree(std::move(pointer));
    }
}

std::unique_ptr<record> BPTree::find(keyType key)
{
}

std::unique_ptr<node> BPTree::find_leaf(keyType key)
{
    auto root = fm.fileHeader.rootPageNumber;
    if (root == EMPTY_PAGE_NUMBER)
    {
        if (verbose_output)
        {
            // 로그 출력
        }
        return nullptr;
    }

    auto now = std::make_unique<node>();
    ASSERT_WITH_LOG(fm.pageRead(root, *now), nullptr, "page read");
}

std::unique_ptr<record> BPTree::make_record(keyType key,
                                            const val_t& value) const
{
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

bool BPTree::start_new_tree(std::unique_ptr<record> record)
{
    auto root = make_node(true);

    root->insert_record(std::move(record), 0);

    auto pagenum = fm.pageCreate();
    ASSERT_WITH_LOG(pagenum != EMPTY_PAGE_NUMBER, false, "page creation failure");
    ASSERT_WITH_LOG(fm.pageWrite(pagenum, *root), false, "page write failure");

    fm.fileHeader.rootPageNumber = pagenum;
    ASSERT_WITH_LOG(fm.updateFileHeader(), false, "file header update failure");

    return true;
}
