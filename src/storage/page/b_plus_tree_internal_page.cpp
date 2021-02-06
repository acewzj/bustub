//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  //
  SetSize(1);
  //
  SetPageId(page_id);
  //
  SetParentPageId(parent_id);
  //
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(0 <= index && index < GetSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(0 <= index && index < GetSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const { 
  for (int i = 0; i < GetSize(); ++i)
  {
    if (array[i].second == value)
    {
      return i;
    }
  }
  return GetSize();
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { 
  assert(0 <= index && index < GetSize());
  return array[index].second;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  assert(0 <= index && index < GetSize());
  array[index].second = value;
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  assert(GetSize() > 1);
  if (comparator(key, array[1].first) < 0) {
    return array[0].second;
  }
  else if (comparator(key, array[GetSize() - 1].first) >= 0) {
    return array[GetSize() - 1].second;
  }  
  int low = 1, high = GetSize() - 1, mid;
  while (low <= high) {
    // 二分查找 左闭右闭
    mid = low + (high - low) / 2;
    if (comparator(key, array[mid].first) < 0) {
      high = mid - 1;
    }
    else if (comparator(key, array[mid].first) > 0) {
      low = mid + 1;
    }else {
      return array[mid].second;
    }
  }
  return array[low].second;

}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * 使用old_value + new_key & new_value 在新的根页面进行殖民
 * 当插入造成叶子页面的溢出时，你需要创造一个新的根页面并且殖民其元素
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value) {
  // must be an empty page
  assert(GetSize() == 1);
  array[0].second = old_value;
  array[1] = {new_key, new_value};
  IncreaseSize(1);  
}
/*
 * Insert new_key & new_value pair right after the pair with its value == old_value
 * 插入一个新的 键值对 到 value == old_value 的后面
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  for (int i = GetSize(); i > 0; --i) {
    if (array[i - 1].second == old_value) {
      // 在old_value节点后面插入一个新节点
      array[i] = {new_key, new_value};
      IncreaseSize(1);
      break;
    }
    array[i] = array[i - 1];
  }
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  auto half = (GetSize() + 1) / 2;
  recipient->CopyNFrom(array + GetSize() - half, half, buffer_pool_manager);

  // 更新孩子节点的父节点id
  for (auto index = GetSize() - half; index < GetSize(); ++index)
  {
    auto *page = buffer_pool_manager->FetchPage(ValueAt(index));
    if (page == nullptr)
    {
      throw Exception(ExceptionType::OUT_OF_MEMORY,
                      "all page are pinned while CopyLastFrom");
    }
    auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
    child->SetParentPageId(recipient->GetPageId());

    assert(child->GetParentPageId() == recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
  }
  IncreaseSize(-1 * half);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  assert(!IsLeafPage() && GetSize() == 1 && size > 0);
  for (int i = 0; i < size; ++i)
  {
    array[i] = *items++;
  }
  IncreaseSize(size - 1);  
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  assert(0 <= index && index < GetSize());
  for (int i = index; i < GetSize() - 1; ++i)
  {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);  
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  IncreaseSize(-1);
  assert(GetSize() == 1);
  return ValueAt(0);  
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const int index_in_parent, BufferPoolManager *buffer_pool_manager) {
  // 首先先获得父节点页面
  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while MoveAllTo");
  }  
  auto *parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());  
  // 更新父节点中的key值
  SetKeyAt(0, parent->KeyAt(index_in_parent));  

  assert(parent->ValueAt(index_in_parent) == GetPageId());

  buffer_pool_manager->UnpinPage(parent->GetPageId(), true);

  recipient->CopyNFrom(array, GetSize(), buffer_pool_manager);  
  // 更新孩子节点的父节点id
  for (auto index = 0; index < GetSize(); ++index) {
    auto *page = buffer_pool_manager->FetchPage(ValueAt(index));
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY,
                      "all page are pinned while CopyLastFrom");
    }
    auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());

    child->SetParentPageId(recipient->GetPageId());

    assert(child->GetParentPageId() == recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
  }  
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() > 1);
  MappingType pair{KeyAt(1), ValueAt(0)};
  page_id_t child_page_id = ValueAt(0);
  SetValueAt(0, ValueAt(1));
  Remove(1);
  recipient->CopyLastFrom(pair, buffer_pool_manager);
  
  auto *page = buffer_pool_manager->FetchPage(child_page_id);  // 更新孩子节点的父节点id
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while MoveFirstToEndOf");
  }
  auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(recipient->GetPageId());
  assert(child->GetParentPageId() == recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);  
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() + 1 <= GetMaxSize());
  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "All pages are pinned while CopyLastFrom");
  }
  auto parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  auto index = parent->ValueIndex(GetPageId());
  auto key = parent->KeyAt(index + 1); // 取得父节点中 value == 本人page_id 的 key 
  array[GetSize()] = {key, pair.second}; // 不应该是 {pair.first, pair.second}吗？哈哈，仔细看索引index：大于index 的会被引领到index + 1!
  IncreaseSize(1);
  parent->SetKeyAt(index + 1, pair.first);

  buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}


/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 * 移动该页最后的键值对到 recipient 页的头部
 * 你需要处理原始的虚拟键-1，例如：更新 recipient 页的数组
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, int parent_index, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() > 1);
  IncreaseSize(-1);// 减少该页的数量-1
  MappingType pair = array[GetSize()]; // 取得最后的键值对
  page_id_t child_page_id = pair.second; // 取得最后的键值对指向的孩子页面 ID
  recipient->CopyFirstFrom(pair, parent_index, buffer_pool_manager); // 将 pair 插入到 recipient 的 -1 的右面第一个
  auto *page = buffer_pool_manager->FetchPage(child_page_id); // 更新孩子节点的父节点 ID
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "All pages are pinned while CopyLastFrom");
  }
  auto child = reinterpret_cast<BPlusTreePage *> (page->GetData());
  child->SetParentPageId(recipient->GetPageId());

  assert(child->GetParentPageId() == recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 * 在起始处附加一条记录
 * 由于这是一个内部页面，被移动的记录所在的页面的父页面需要被更新
 * 所以需要调整该条记录所在页面的父页面ID，同时需要和BPM一致
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, int parent_index, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() + 1 < GetMaxSize()); // 断言当前页面的记录数量能给新纪录留下足够的空间，如果空间不够了的话直接就退出了
  auto* page = buffer_pool_manager->FetchPage(GetParentPageId()); // 从BPM中取出当前页面的父亲页面ID，好进行更新
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "All pages are pinned while CopyFirstFrom");
  }
  auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *> (page->GetData()); // 取出父亲页面 Data 部分的数据重新翻译为叶子节点数据
  auto key = parent->KeyAt(parent_index); // 取出在 parent_index 处的 key
  parent->SetKeyAt(parent_index, pair.first); // 设置 parent_index 处新的 key
  InsertNodeAfter(array[0].second, key, array[0].second); // 在 -1 的后面插入新的键值对 key-array[0].second;
  // 此处为什么不采用 InsertNodeAfter(array[0].second, key, pair.second); ？
  // 因为 InsertNodeAfter 函数是通过寻找 old_value == new_value 来将 新的 键值对 插入到 -1 的右面，然后再更新 value 就可以了
  array[0].second = pair.second;
  buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
