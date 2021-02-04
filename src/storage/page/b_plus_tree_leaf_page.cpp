//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  // 1 for the first invalid key
  SetSize(0);
  //
  SetPageId(page_id);
  //
  SetParentPageId(parent_id);
  //
  SetNextPageId(INVALID_PAGE_ID);
  //
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const { 
  for (int i = 0; i < GetSize(); ++i) {
    if (comparator(key, array[i].first) <= 0) {
      return i;
    }
  }
  return GetSize();  
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(0 <= index && index < GetSize());
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  assert(0 <= index && index < GetSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  //要插入的值比当前最大的还大，那么插入到下一个地方
  if (GetSize() == 0 || comparator(key, KeyAt(GetSize() - 1)) > 0) {
    array[GetSize()] = {key, value};
  }
  // 要插入的值比当前最小的还小,赶紧给我腾位置 array 
  else if (comparator(key, array[0].first) < 0) {
    memmove(array + 1, array, static_cast<size_t>(GetSize() * sizeof(MappingType)));
    array[0] = {key, value};
  }
  else {
    int low = 0, high = GetSize() - 1, mid;
    while (low <= high) {
      mid = low + (high - low) / 2;
      if (comparator(key, KeyAt(mid)) > 0) {
        low = mid + 1;
      }
      else if (comparator(key, KeyAt(mid)) < 0) {
        high = mid - 1;
      }
      else {
        // 由于不支持重复的key， 所以不应该执行到这里
        assert(0);
      }
    }
    //找到该插入的位置了为high，接下来将它右面的东西移动一下
    memmove(array + high + 1, array + high, static_cast<size_t>(GetSize() - high) * sizeof(MappingType));
    array[high] = {key, value};
  }

  IncreaseSize(1);
  assert(GetSize() <= GetMaxSize());
  return GetSize();    
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  assert(GetSize() > 0);
  int size = GetSize() / 2;
  MappingType *src = array + GetSize() - size;
  recipient->CopyNFrom(src, size);
  this->IncreaseSize(-1 * size);
}
 
/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  assert(IsLeafPage() && (GetSize() + size <= GetMaxSize()));
  // 这个函数从别的叶子页面拷贝过来会不会把自己的一些东西也给覆盖了呢？
  // 我觉得不能覆盖之前写的东西了吧？
  auto start = GetSize();
  for (int i = 0; i < size; ++i) {
    array[start + i] = *items++;
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const {
  if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 || comparator(key, KeyAt(GetSize() - 1)) > 0)
    return false;
  int low = 0, high = GetSize() - 1, mid;
  while (low <= high) {
    mid = low + (high - low) / 2;
    if (comparator(key, KeyAt(mid)) > 0) {
      low = mid + 1;
    }
    else if (comparator(key, KeyAt(mid)) < 0) {
      high = mid - 1;
    }
    else {
      value = array[mid].second;
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 || comparator(key, KeyAt(GetSize() - 1)) > 0 ) {
    return GetSize();
  }
  int low = 0, high = GetSize() - 1, mid;
  while (low <= high) {
    mid = low + (high - low) / 2;
    if (comparator(key, KeyAt(mid)) > 0) {
      low = mid + 1;
    }
    else if (comparator(key, KeyAt(mid)) < 0) {
      high = mid - 1;
    }
    else {
      memmove(array + mid, array + mid + 1, static_cast<size_t>((GetSize() - mid - 1) * sizeof(MappingType)));
      IncreaseSize(-1);
      break;
    }
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  recipient->CopyNFrom(array, GetSize());
  // ?这块不应该是从 x 拷贝 到 y 吗？不应该是设置 x 的 NextPageId？怎么变成了设置 y 的 NextPageId？？？
  recipient->SetNextPageId(GetNextPageId());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {

}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {

}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {

}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
