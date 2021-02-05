//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
thread_local bool BPlusTree<KeyType, ValueType, KeyComparator>::root_is_locked = false;
/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { 
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  // 根据 key 找到叶子节点页面
  auto* leaf = FindLeafPage(key, false, Operation::READONLY, transaction);
  bool ret = false;
  if (leaf != nullptr) {
    ValueType value;
    if (leaf->Lookup(key, value, comparator_)) {
      result->push_back(value);
      ret = true;
    }
    // 释放锁
    UnlockUnpinPages(Operation::READONLY, transaction);

    if (transaction == nullptr) {
      auto page_id = leaf->GetPageId();
      buffer_pool_manager_->FetchPage(page_id)->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, false);
    }
  }
  return ret;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

// **************** lab3 ***********************
/*
 * Unlock all nodes current transaction hold according to op then unpin pages
 * and delete all pages if any
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTree<KeyType, ValueType, KeyComparator>::
      UnlockUnpinPages(Operation op, Transaction* transaction) {
  if (transaction == nullptr) {
    return;
  }
  for (auto* page : *transaction->GetPageSet()) {
    if (op == Operation::READONLY) {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }

  transaction->GetPageSet()->clear();

  for (auto page_id : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page_id);
  }

  transaction->GetDeletedPageSet()->clear();

  if (root_is_locked) {
    root_is_locked = false;
    unlockRoot();
  }
}
/*
 * Note: leaf node and internal node have different MAXSIZE
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::isSafe(BPlusTreePage* node, Operation op) {
  if (op == Operation::INSERT) {
    return node->GetSize() < node->GetMaxSize();
  }
  else if (op == Operation::DELETE) {
    return node->GetSize() > node->GetMinSize() + 1;
  }
  return true;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */ 
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {  
  if (IsEmpty()) {
    std::lock_guard<std::mutex> lock(mutex_);      
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto *page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all pages pinned while StartNewTree");
  }
  // 
  auto root = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
  // 别忘了要更新根节点页面id
  UpdateRootPageId(true);
  //set max page size, header is 28bytes 24 + 4 next_page_id_ (4096 - 28) / 16
  // int size = (PAGE_SIZE - sizeof(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>)) / (sizeof(KeyType) + sizeof(ValueType));  
  root->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  root->Insert(key, value, comparator_);
  //根页已经被修改了，写入了东西。
  buffer_pool_manager_->UnpinPage(root->GetPageId(), true);  
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  auto* leaf = FindLeafPage(key, false, Operation::INSERT, transaction);
 
  if (leaf == nullptr)
    return false;  
  ValueType v;
  // 如果树中已经存在值了，有key了，就返回false
  if (leaf->Lookup(key, v, comparator_)) {
    UnlockUnpinPages(Operation::INSERT, transaction);
    return false;
  }
  // 不需要分裂就直接插入
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    leaf->Insert(key, value, comparator_);
  } 
  else {
    // 分裂出一个新的叶子节点页面
    auto* leaf2 = Split<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>(leaf);
    if (comparator_(key, leaf2->KeyAt(0)) < 0) {
      leaf->Insert(key, value, comparator_);
    } 
    else {
      leaf2->Insert(key, value, comparator_);
    }
    // 更新前后关系
    if (comparator_(leaf->KeyAt(0), leaf2->KeyAt(0)) < 0) {
      leaf2->SetNextPageId(leaf->GetNextPageId());
      leaf->SetNextPageId(leaf2->GetPageId());
    } 
    else {
      leaf2->SetNextPageId(leaf->GetPageId());
    }
    // 将分裂的节点插入到父节点
    InsertIntoParent(leaf, leaf2->KeyAt(0), leaf2, transaction);    
  }
  UnlockUnpinPages(Operation::INSERT, transaction);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id;
  auto* page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while Splitting.");    
  }
  auto new_node = reinterpret_cast<N *>(page->GetData());
  new_node->Init(page_id, node->GetPageId(), node->GetMaxSize());

  node->MoveHalfTo(new_node);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // 如果 old_node 是根节点，则需要重新生成一个根页面，页面 id 即为 root_page_id_
  if (old_node->IsRootPage()) {
    auto* page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while InsertIntoParent");
    }
    assert(page->GetPinCount() == 1);
    auto* root = reinterpret_cast<BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *> (page->GetData());
    root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    root->PopulateNewRoot(static_cast<ValueType>(old_node->GetPageId()), key, static_cast<ValueType>(new_node->GetPageId()));

    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);    

    // 这时需要更新根节点页面id
    UpdateRootPageId(false);   
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

    buffer_pool_manager_->UnpinPage(root->GetPageId(), true);     
  }          
  else {
    auto *page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while InsertIntoParent");
    }
    auto* internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
    // 如果父节点还有空间
    if (internal->GetSize() < internal->GetMaxSize()) {
      internal->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      new_node->SetParentPageId(internal->GetPageId());
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);      
    }
    else {
      page_id_t page_id;
      auto *page = buffer_pool_manager_->NewPage(&page_id);
      if (page == nullptr)
      {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while InsertIntoParent");
      }
      assert(page->GetPinCount() == 1);
      auto *copy = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());   
      copy->Init(page_id, INVALID_PAGE_ID, internal_max_size_);
      copy->SetSize(internal->GetSize());
      for (int i = 1, j = 0; i <= internal->GetSize(); ++i, ++j) {
        if (internal->ValueAt(i - 1) == old_node->GetPageId()) {
          copy->SetKeyAt(j, key);
          copy->SetValueAt(j, new_node->GetPageId());
          ++j;
        }
        if (i < internal->GetSize()) {
          copy->SetKeyAt(j, internal->KeyAt(i));
          copy->SetValueAt(j, internal->ValueAt(i));
        }
      }

      assert(copy->GetSize() == copy->GetMaxSize());
      auto internal2 = Split<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>> (copy);

      internal->SetSize(copy->GetSize() + 1);
      for (int i = 0; i < copy->GetSize(); ++i) {
        internal->SetKeyAt(i + 1, copy->KeyAt(i));
        internal->SetValueAt(i + 1, copy->ValueAt(i));
      }
      if (comparator_(key, internal2->KeyAt(0)) < 0) {
        new_node->SetParentPageId(internal->GetPageId());
      }
      else if (comparator_(key, internal2->KeyAt(0)) == 0) {
        new_node->SetParentPageId(internal2->GetPageId());
      }
      else {
        new_node->SetParentPageId(internal2->GetPageId());
        old_node->SetParentPageId(internal2->GetPageId());
      }
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

      buffer_pool_manager_->UnpinPage(copy->GetPageId(), false);
      buffer_pool_manager_->DeletePage(copy->GetPageId());

      InsertIntoParent(internal, internal2->KeyAt(0), internal2);      
    }
    buffer_pool_manager_->UnpinPage(internal->GetPageId(), true);    
  }                         

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) return;
  auto *leaf = FindLeafPage(key, false, Operation::DELETE, transaction);
  if (leaf != nullptr) {
    int size_before_deletion = leaf->GetSize();
    if (leaf->RemoveAndDeleteRecord(key, comparator_) != size_before_deletion) {
      if (CoalesceOrRedistribute(leaf, transaction)) {
        transaction->AddIntoDeletedPageSet(leaf->GetPageId());
      }
    }
    UnlockUnpinPages(Operation::DELETE, transaction);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  if (node->IsLeafPage()) {
    if (node->GetSize() >= node->GetMinSize()) {
      return false;
    }
  }
  else {
    if (node->GetSize() > node->GetMinSize()) {
      return false;
    }
  }
  auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY,
                    "all page are pinned while CoalesceOrRedistribute");
  }
  auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
  int value_index = parent->ValueIndex(node->GetPageId());
  assert(value_index != parent->GetSize());
  int sibling_page_id;
  if (value_index == 0) {
    sibling_page_id = parent->ValueAt(value_index + 1);
  }
  else {
    sibling_page_id = parent->ValueAt(value_index - 1);
  }
  page = buffer_pool_manager_->FetchPage(sibling_page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while CoalesceOrRedistribute");
  }
  // lab3
  page->WLatch();
  transaction->AddIntoPageSet(page);
  auto sibling = reinterpret_cast<N *>(page->GetData());
  bool redistribute = false;
  if (sibling->GetSize() + node->GetSize() > node->GetMaxSize()) {
    redistribute = true;
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  }
  if (redistribute) {
    if (value_index == 0) {
      Redistribute<N>(sibling, node, 1);
    }
    return false;
  }
  bool ret;
  if (value_index == 0) {
    // lab3
    Coalesce<N>(node, sibling, parent, 1, transaction);
    transaction->AddIntoDeletedPageSet(sibling_page_id);
    ret = false;
  }
  else {
    Coalesce<N>(sibling, node, parent, value_index, transaction);
    ret = true;
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  return ret;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * 移动所有的键值对 从一个页面到它的兄弟页面，并且通知BPM 去删除该页，父页面必须进行调整以考虑删除信息 必要时递归重新分配
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                              Transaction *transaction) {
  node->MoveAllTo(neighbor_node, index);

  parent->Remove(index);

  if (CoalesceOrRedistribute(parent, transaction))
  {
    transaction->AddIntoDeletedPageSet(parent->GetPageId());
    return true;
  }                                 
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * 重新分配 键值对 从一个页面到 兄弟页面， 如果索引为0 ，移动兄弟页面的首个键值对到输入节点的最后面
 * 否则移动兄弟页面的最后的键值对到输入节点的头部
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node);
  }
  else {
    auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY,
                      "all page are pinned while Redistribute");
    }
    auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
    int idx = parent->ValueIndex(node->GetPageId());
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
    neighbor_node->MoveLastToFrontOf(node, idx);
  }  
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // 如果删除了最后一个节点
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
      return true;
    }
    return false;
  }
  // 删除了还有最后一个节点
  if (old_root_node->GetSize() == 1) {
    auto root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node);
    root_page_id_ = root->ValueAt(0);
    UpdateRootPageId(false);
    auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while AdjustRoot");
    }
    auto new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  return false;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf = FindLeafPage(key, false);
  int index = 0;
  if (leaf != nullptr) {
    index = leaf->KeyIndex(key, comparator_);
  }
  return IndexIterator<KeyType, ValueType, KeyComparator>(leaf, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, Operation op, Transaction *transaction) {

  // 如果操作不是只读的，就要把根节点锁住，防止把根节点给篡改了
  if (op != Operation::READONLY) {
    lockRoot();
    root_is_locked = true;
  }
  if (IsEmpty()) {
    return nullptr;
  }

  auto *parent = buffer_pool_manager_->FetchPage(root_page_id_);

  if (parent == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while FindLeafPage");
  }

  if (op == Operation::READONLY) {
    parent->RLatch();
  }
  else {
    parent->WLatch();
  }
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(parent);
  }
  auto* node = reinterpret_cast<BPlusTreePage *> (parent->GetData());
  while (!node->IsLeafPage()) {
    auto * internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    page_id_t parent_page_id = node->GetPageId(), child_page_id;

    if (leftMost) {
      child_page_id = internal->ValueAt(0);
    }
    else {
      child_page_id = internal->Lookup(key, comparator_);
    }
    
    auto* child = buffer_pool_manager_->FetchPage(child_page_id);
    if (child == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while FindLeafPage");
    }
    if (op == Operation::READONLY) {
      child->RLatch();
      UnlockUnpinPages(op, transaction);
    } else {
      child->WLatch();
    }
    node = reinterpret_cast<BPlusTreePage *>(child->GetData());
    assert(node->GetParentPageId() == parent_page_id);

    // 如果是安全的，就释放父节点那的锁
    if (op != Operation::READONLY && isSafe(node, op)) {
      UnlockUnpinPages(op, transaction);
    }
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(child);
    } else {
      parent->RUnlatch();
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
      parent = child;
    }    
  }  
 
  return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node);  
}

/* 
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
