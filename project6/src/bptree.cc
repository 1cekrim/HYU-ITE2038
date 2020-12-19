#include "bptree.hpp"

#include <algorithm>
#include <cstring>
#include <iostream>
#include <vector>

#include "log_manager.hpp"
#include "logger.hpp"
#include "scoped_page_latch.hpp"

scoped_node_latch::scoped_node_latch(int manager_id, nodeId_t id, int buffer_index)
    : manager_id(manager_id),
      id(id),
      buffer_index(buffer_index),
      locked(false)
{
    lock();
}

scoped_node_latch::~scoped_node_latch()
{
    unlock();
} 

void scoped_node_latch::lock()
{
    if (locked)
    {
        return;
    }
    locked = true;
    auto& frame = BufferController::instance().at(buffer_index);
    DB_CRASH_COND(frame.file_id == manager_id && frame.pagenum == id, -1, "invalid frame");
    frame.mtx.lock();
    if (frame.pin != 0)
    {
        std::cout << "???: " << frame.pin << std::endl;
    }
    ++frame.pin;
}

void scoped_node_latch::unlock()
{
    if (!locked)
    {
        std::cout << "???";
        return;
    }
    auto& frame = BufferController::instance().at(buffer_index);
    DB_CRASH_COND(frame.file_id == manager_id && frame.pagenum == id, -1, "invalid frame");
    --frame.pin;
    if (frame.pin != 0)
    {
        std::cout << "???: " << frame.pin << std::endl;
    }
    frame.mtx.unlock();
    locked = false;
}

scoped_node_latch_shared::scoped_node_latch_shared(int manager_id, nodeId_t id, int buffer_index)
    : manager_id(manager_id),
      id(id),
      buffer_index(buffer_index),
      locked(false)
{
    lock_shared();
}

scoped_node_latch_shared::~scoped_node_latch_shared()
{
    unlock_shared();
}

void scoped_node_latch_shared::lock_shared()
{
    if (locked)
    {
        return;
    }
    locked = true;
    auto& frame = BufferController::instance().at(buffer_index);
    DB_CRASH_COND(frame.file_id == manager_id && frame.pagenum == id, -1, "invalid frame");
    frame.mtx.lock_shared();
    ++frame.pin;
}

void scoped_node_latch_shared::unlock_shared()
{
    if (!locked)
    {
        std::cout << "???";
        return;
    }
    auto& frame = BufferController::instance().at(buffer_index);
    DB_CRASH_COND(frame.file_id == manager_id && frame.pagenum == id, -1, "invalid frame");
    --frame.pin;
    frame.mtx.unlock_shared();
    locked = false;
}

BPTree::BPTree(bool verbose_output, int delayed_min)
    : leaf_order(LEAF_ORDER),
      internal_order(INTERNAL_ORDER),
      verbose_output(verbose_output),
      delayed_min(delayed_min)
{
    // Do nothing
}

void BPTree::set_table(int table_id)
{
    this->table_id = table_id;
}

bool BPTree::open_table(const std::string &filename)
{
    CHECK_WITH_LOG(manager.open(filename), false, "open table failure");
    return true;
}

int BPTree::char_to_valType(valType &dst, const char *src) const
{
    std::fill(std::begin(dst), std::end(dst), 0);
    std::copy_n(src, std::min(static_cast<int>(strlen(src)), 119),
                std::begin(dst));
    return 0;
}

int BPTree::get_table_id() const
{
    return table_id;
}

bool BPTree::insert(keyType key, const valType &value)
{
    // 중복을 허용하지 않음
    if (exist_key(key))
    {
        return false;
    }

    record_t record;
    record = {key, value};

    if (!is_valid(manager.root()))
    {
        return start_new_tree(record);
    }

    node_tuple leaf;
    CHECK(find_leaf(key, leaf));

    /*
    현재는 insert를 thread-safe 하지 않게 구현하므로, latch를 제거
    */
    // scoped_node_latch leaf_latch {manager.get_manager_id(), leaf.id};

    if (leaf.node.number_of_keys() < leaf_order - 1)
    {
        bool result = insert_into_leaf(leaf, record);
        return result;
    }

    bool result = insert_into_leaf_after_splitting(leaf, record);
    return result;
}

bool BPTree::update(keyType key, const valType &value, int transaction_id)
{
    std::unique_lock<std::mutex> buffer_latch{
        BufferController::instance().mtx};

    if (!exist_key(key))
    {
        return false;
    }

    node_tuple leaf;
    CHECK(find_leaf(key, leaf));

    scoped_node_latch latch{manager.get_manager_id(), leaf.id, leaf.buffer_index};

    if (transaction_id != TransactionManager::invliad_transaction_id)
    {
        std::unique_lock<std::mutex> trxmanager_latch{
            TransactionManager::instance().mtx};
        auto &trx = TransactionManager::instance().get(transaction_id);
        auto [lock, state] = TransactionManager::instance().lock_acquire(
            get_table_id(), key, transaction_id, LockMode::EXCLUSIVE);
        switch (state)
        {
        case LockState::INVALID:
            return false;
        case LockState::ACQUIRED:
            break;
        case LockState::ABORTED:
            // abort 시에도 page latch를 계속 잡고 있어야 한다.
            // abort 할 때 해당 페이지가 중간에 eviction 되면 버그가
            // 발생한다.
            TransactionManager::instance().abort(transaction_id);
            return false;
        case LockState::WAITING:
            trx.mtx.lock();
            trxmanager_latch.unlock();
            latch.unlock();
            buffer_latch.unlock();
            trx.mtx.unlock();
            TransactionManager::instance().lock_wait(lock);
            buffer_latch.lock();
            latch.lock();
        }
    }

    int idx = leaf.buffer_index;
    auto& frame = BufferController::instance().at(idx);

    buffer_latch.unlock();

    valType before;

    int i = frame.index_key<record_t>(key);
    before = frame.records()[i].value;
    frame.records()[i].value = value;

    auto lsn = LogManager::instance().update_log(
        transaction_id, manager.get_manager_id(), leaf.id,
        frame.get_offset<record_t>(i), sizeof(valType), before, value);

    frame.nodePageHeader().pageLsn = lsn;
    frame.is_dirty = true;

    return true;
}

bool BPTree::insert_into_leaf(node_tuple &leaf, const record_t &rec)
{
    int insertion_point =
        leaf.node.satisfy_condition_first<record_t>([&rec](auto &now) {
            return now.key >= rec.key;
        });
    leaf.node.insert(rec, insertion_point);

    CHECK(commit_node(leaf));

    return true;
}

bool BPTree::insert_into_leaf_after_splitting(node_tuple &leaf,
                                              const record_t &rec)
{
    node_tuple new_leaf{create_node()};
    new_leaf.node.set_is_leaf(true);
    int insertion_index =
        leaf.node.satisfy_condition_first<record_t>([&rec](auto &now) {
            return now.key >= rec.key;
        });

    std::vector<record_t> temp;
    temp.reserve(leaf_order + 1);

    auto back = std::back_inserter(temp);

    leaf.node.range_copy<record_t>(back, 0, insertion_index);
    back = rec;
    leaf.node.range_copy<record_t>(back, insertion_index);

    int split = cut(leaf_order - 1);

    leaf.node.range_assignment<record_t>(temp.begin(), temp.begin() + split);
    new_leaf.node.range_assignment<record_t>(temp.begin() + split, temp.end());

    new_leaf.node.set_next_leaf(leaf.node.next_leaf());
    leaf.node.set_next_leaf(new_leaf.id);
    new_leaf.node.set_parent(leaf.node.parent());

    CHECK(commit_node(leaf));
    CHECK(commit_node(new_leaf));

    return insert_into_parent(leaf, new_leaf.node.get<record_t>(0).key,
                              new_leaf);
}

bool BPTree::insert_into_parent(node_tuple &left, keyType key,
                                node_tuple &right)
{
    node_tuple parent;
    parent.id = left.node.parent();
    if (!is_valid(parent.id))
    {
        return insert_into_new_root(left, key, right);
    }

    CHECK(load_node(parent));

    int left_index = get_left_index(parent.node, left.id);
    if (static_cast<int>(parent.node.nodePageHeader().numberOfKeys) <
        internal_order - 1)
    {
        return insert_into_node(parent, left_index, key, right);
    }

    return insert_into_node_after_splitting(parent, left_index, key, right);
}

int BPTree::get_left_index(const node_t &parent, nodeId_t left_id) const
{
    if (parent.leftmost() == left_id)
    {
        return 0;
    }

    int left_index =
        parent.satisfy_condition_first<internal_t>([&left_id](auto &now) {
            return now.node_id == left_id;
        });
    return left_index + 1;
}

bool BPTree::load_node(node_tuple &target)
{
    int result = manager.load(target.id, target.node);
    CHECK_WITH_LOG(result != INVALID_BUFFER_INDEX, false,
                   "load node failure: %ld", target.id);
    target.buffer_index = result;
    return true;
}

bool BPTree::commit_node(nodeId_t node_id, const node_t &node)
{
    CHECK_WITH_LOG(manager.commit(node_id, node), false,
                   "commit node failure: %ld", node_id);
    return true;
}

bool BPTree::commit_node(const node_tuple &target)
{
    CHECK_WITH_LOG(manager.commit(target.id, target.node), false,
                   "commit node failure: %ld", target.id);
    return true;
}

bool BPTree::free_node(node_tuple &target)
{
    CHECK_WITH_LOG(manager.free(target.id), false, "free page failure: %ld",
                   target.id);
    return true;
}

nodeId_t BPTree::create_node()
{
    nodeId_t t = manager.create();
    CHECK_WITH_LOG(t != INVALID_NODE_ID, INVALID_NODE_ID,
                   "create node failure");
    return t;
}

bool BPTree::insert_into_new_root(node_tuple &left, keyType key,
                                  node_tuple &right)
{
    node_tuple root = {create_node()};
    CHECK(root);

    root.node.set_leftmost(left.id);
    root.node.emplace_back<internal_t>(key, right.id);

    left.node.set_parent(root.id);
    right.node.set_parent(root.id);

    CHECK(commit_node(left));
    CHECK(commit_node(right));
    CHECK(commit_node(root));

    CHECK(manager.set_root(root.id));

    return true;
}

bool BPTree::insert_into_node(node_tuple &parent, int left_index, keyType key,
                              node_tuple &right)
{
    CHECK_WITH_LOG(left_index >= 0 && left_index < 248, false, "left_index: %d",
                   left_index);
    parent.node.insert<internal_t>({key, right.id}, left_index);
    CHECK(commit_node(parent));
    return true;
}

bool BPTree::insert_into_node_after_splitting(node_tuple &parent,
                                              int left_index, keyType key,
                                              node_tuple &target)
{
    node_tuple right = {create_node()};
    CHECK(right);

    std::vector<internal_t> temp;
    temp.reserve(internal_order);

    auto back = std::back_inserter(temp);
    parent.node.range_copy<internal_t>(back, 0, left_index);
    back = {key, target.id};
    parent.node.range_copy<internal_t>(back, left_index);

    int split = cut(internal_order);

    keyType k_prime = temp[split - 1].key;

    parent.node.range_assignment<internal_t>(temp.begin(),
                                             temp.begin() + split - 1);

    right.node.set_leftmost(temp[split - 1].node_id);
    right.node.range_assignment<internal_t>(temp.begin() + split, temp.end());
    right.node.set_parent(parent.node.parent());

    CHECK(update_parent_with_commit(right.node.leftmost(), right.id));
    for (auto &tmp : right.node.range<internal_t>())
    {
        CHECK(update_parent_with_commit(tmp.node_id, right.id));
    }

    CHECK(commit_node(parent));
    CHECK(commit_node(right));

    return insert_into_parent(parent, k_prime, right);
}

bool BPTree::delete_key(keyType key)
{
    if (!exist_key(key))
    {
        return false;
    }

    node_tuple leaf;
    CHECK(find_leaf(key, leaf));

    CHECK_WITH_LOG(delete_entry(leaf, key), false, "delete entry failure");

    return true;
}

bool BPTree::delete_entry(node_tuple &target, keyType key)
{
    CHECK_WITH_LOG(remove_entry_from_node(target, key), false,
                   "remove entry from node failure: %ld", key);
    CHECK(commit_node(target));

    if (target.id == manager.root())
    {
        return adjust_root(target);
    }

    int min_keys;
    if (delayed_min)
    {
        min_keys = delayed_min;
    }
    else
    {
        min_keys =
            target.node.is_leaf() ? cut(leaf_order - 1) : cut(leaf_order) - 1;
    }

    if (static_cast<int>(target.node.number_of_keys()) >= min_keys)
    {
        return true;
    }

    node_tuple parent;
    parent.id = target.node.parent();
    CHECK(load_node(parent));
    CHECK(parent);
    CHECK(parent.node.number_of_keys() > 0);

    int neighbor_index = get_left_index(parent.node, target.id) - 1;
    int k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
    keyType k_prime = parent.node.get<internal_t>(k_prime_index).key;

    node_tuple neighbor;
    neighbor.id =
        neighbor_index == -1
            ? parent.node.get<internal_t>(0).node_id
            : neighbor_index == 0
                  ? parent.node.leftmost()
                  : parent.node.get<internal_t>(neighbor_index - 1).node_id;
    CHECK(load_node(neighbor));

    int capacity = target.node.is_leaf() ? leaf_order : internal_order - 1;

    if (static_cast<int>(target.node.number_of_keys() +
                         neighbor.node.number_of_keys()) < capacity)
    {
        if (neighbor_index == -1)
        {
            return coalesce_nodes(neighbor, target, parent, k_prime);
        }
        else
        {
            return coalesce_nodes(target, neighbor, parent, k_prime);
        }
    }
    else
    {
        return redistribute_nodes(target, neighbor, parent, k_prime,
                                  k_prime_index, neighbor_index);
    }
}

bool BPTree::remove_entry_from_node(node_tuple &target, keyType key)
{
    if (target.node.is_leaf())
    {
        int idx = target.node.index_key<record_t>(key);
        CHECK_WITH_LOG(idx != -1, false, "invalid key: %ld", key);
        target.node.erase<record_t>(idx);
    }
    else
    {
        int idx = target.node.index_key<internal_t>(key);
        CHECK_WITH_LOG(idx != -1, false, "invalid key: %ld", key);
        target.node.erase<internal_t>(idx);
    }

    return true;
}

bool BPTree::adjust_root(node_tuple &root)
{
    if (root.node.number_of_keys() > 0)
    {
        return true;
    }

    if (root.node.is_leaf())
    {
        CHECK(manager.set_root(INVALID_NODE_ID));
    }
    else
    {
        nodeId_t new_root_id = root.node.leftmost();
        CHECK(manager.set_root(new_root_id));

        CHECK(update_parent_with_commit(new_root_id, INVALID_NODE_ID));
    }

    CHECK(free_node(root));

    return true;
}

bool BPTree::update_parent_with_commit(nodeId_t target_id, nodeId_t parent_id)
{
    node_tuple temp{target_id};
    CHECK(load_node(temp));
    temp.node.set_parent(parent_id);
    CHECK(commit_node(temp));

    return true;
}

bool BPTree::coalesce_nodes(node_tuple &target, node_tuple &neighbor,
                            node_tuple &parent, int k_prime)
{
    if (target.node.is_leaf())
    {
        for (auto &rec : target.node.range<record_t>())
        {
            neighbor.node.push_back(rec);
        }
        neighbor.node.set_next_leaf(target.node.next_leaf());
    }
    else
    {
        neighbor.node.emplace_back<internal_t>(k_prime, target.node.leftmost());
        CHECK(update_parent_with_commit(target.node.leftmost(), neighbor.id));
        for (auto &internal : target.node.range<internal_t>())
        {
            neighbor.node.push_back(internal);
            CHECK(update_parent_with_commit(internal.node_id, neighbor.id));
        }
    }

    CHECK(commit_node(neighbor));
    CHECK(delete_entry(parent, k_prime));
    CHECK(free_node(target));

    return true;
}

bool BPTree::redistribute_nodes(node_tuple &target, node_tuple &neighbor,
                                node_tuple &parent, int k_prime,
                                int k_prime_index, int neighbor_index)
{
    if (neighbor_index != -1)
    {
        // left neighbor
        if (!target.node.is_leaf())
        {
            target.node.insert<internal_t>({k_prime, target.node.leftmost()},
                                           0);

            auto &new_one = neighbor.node.back<internal_t>();

            parent.node.get<internal_t>(k_prime_index).key = new_one.key;
            target.node.set_leftmost(new_one.node_id);

            CHECK(update_parent_with_commit(new_one.node_id, target.id));
        }
        else
        {
            target.node.insert(neighbor.node.back<record_t>(), 0);

            parent.node.get<record_t>(k_prime_index).key =
                target.node.first<record_t>().key;
        }
    }
    else
    {
        // right neighbor
        if (!target.node.is_leaf())
        {
            target.node.emplace_back<internal_t>(k_prime,
                                                 neighbor.node.leftmost());

            auto &leftmost = neighbor.node.first<internal_t>();
            parent.node.get<internal_t>(k_prime_index).key = leftmost.key;
            neighbor.node.set_leftmost(leftmost.node_id);

            CHECK(update_parent_with_commit(
                target.node.back<internal_t>().node_id, target.id));

            target.node.erase<internal_t>(0);
        }
        else
        {
            target.node.push_back(neighbor.node.first<record_t>());

            parent.node.get<record_t>(k_prime_index).key =
                neighbor.node.get<record_t>(1).key;

            neighbor.node.erase<record_t>(0);
        }
    }

    CHECK(commit_node(target));
    CHECK(commit_node(neighbor));
    CHECK(commit_node(parent));

    return true;
}

bool BPTree::exist_key(keyType key)
{
    node_tuple leaf;
    if (!find_leaf(key, leaf))
    {
        return false;
    }

    int i = leaf.node.index_key<record_t>(key);
    if (i == -1)
    {
        return false;
    }

    return true;
}

bool BPTree::find(keyType key, record_t &ret, int transaction_id)
{
    std::unique_lock<std::mutex> buffer_latch{
        BufferController::instance().mtx};

    node_tuple leaf;
    if (!find_leaf(key, leaf))
    {
        return false;
    }

    scoped_node_latch_shared latch_shared{manager.get_manager_id(), leaf.id, leaf.buffer_index};

    if (transaction_id != TransactionManager::invliad_transaction_id)
    {
        std::unique_lock<std::mutex> trx_latch{
            TransactionManager::instance().mtx};

        auto &trx = TransactionManager::instance().get(transaction_id);

        auto [lock, state] = TransactionManager::instance().lock_acquire(
            get_table_id(), key, transaction_id, LockMode::SHARED);
        switch (state)
        {
        case LockState::INVALID:
            return false;
        case LockState::ACQUIRED:
            break;
        case LockState::ABORTED:
            // abort 시에는 page latch를 잡고 있어야 할 이유가 없다.
            // latch_shared.unlock_shared();
            TransactionManager::instance().abort(transaction_id);
            return false;
        case LockState::WAITING:
            trx.mtx.lock();
            trx_latch.unlock();
            latch_shared.unlock_shared();
            buffer_latch.unlock();
            trx.mtx.unlock();
            TransactionManager::instance().lock_wait(lock);
            buffer_latch.lock();
            latch_shared.lock_shared();
        }
    }

    buffer_latch.unlock();

    int i = leaf.node.index_key<record_t>(key);
    if (i == -1)
    {
        return false;
    }
 
    ret = leaf.node.get<record_t>(i);

    return true;
}

bool BPTree::find_leaf(keyType key, node_tuple &node)
{
    node.id = manager.root();
    if (!is_valid(node.id))
    {
        return false;
    }

    CHECK(load_node(node));

    // TODO: latch crabbing

    while (!node.node.is_leaf())
    {
        int idx = node.node.key_grt(key) - 1;
        node.id = (idx == -1) ? node.node.leftmost()
                              : node.node.get<internal_t>(idx).node_id;
        CHECK(load_node(node));
    }

    return true;
}

bool BPTree::start_new_tree(const record_t &rec)
{
    node_tuple root = {create_node()};
    root.node.set_is_leaf(true);
    CHECK(root);

    root.node.insert(rec, 0);

    CHECK(commit_node(root));
    CHECK(manager.set_root(root.id));
    return true;
}

bool BPTree::is_valid(nodeId_t node_id) const
{
    return node_id != INVALID_NODE_ID;
}