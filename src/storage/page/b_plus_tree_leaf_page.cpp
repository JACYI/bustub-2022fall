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
#include "storage/index/b_plus_tree.h"

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
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  return next_page_id_;
}


/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

// Find the target key which larger than key first (in order word, the offset of inserting the key)
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  auto target = std::lower_bound(array_, array_ + GetSize(), key, [&comparator](const auto &pair, auto k) {
    return comparator(pair.first, k) < 0;
  });
  // 0 represents = or <, 1,2,... represent the distance between lower bound oftarget and array[0]
  return std::distance(array_, target);
}

// Get key-value pair of index
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> const std::pair<KeyType, ValueType> & {
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &keyComparator) -> int {
  int key_index = KeyIndex(key, keyComparator);
  int cur_size = GetSize();
  if(keyComparator(array_[key_index].first, key) == 0) {
    // if key already exists, give up insert and return
    return cur_size;
  }

  if(key_index < cur_size){
    // Move following items behind
    std::move_backward(array_ + key_index, array_ + cur_size, array_ + GetSize() + 1);
  }

  array_[key_index].first = key;
  array_[key_index].second = value;
  IncreaseSize(1);

  return GetSize();
}


// Get value within key
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &keyComparator) const -> bool {
  int target_in_array = KeyIndex(key, keyComparator);
  if (target_in_array == GetSize() || keyComparator(array_[target_in_array].first, key) != 0) {
    return false;
  }
  *value = array_[target_in_array].second;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &keyComparator) -> int {
  int key_index = KeyIndex(key, keyComparator);
  if(key_index == GetSize() || keyComparator(array_[key_index].first, key) != 0){
    // Not Found
    return GetSize();
  }
  // Move the array ahead
  std::move(array_ + key_index + 1, array_ + GetSize(), array_ + key_index);
  IncreaseSize(-1);

  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SplitAndSnd(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *dest, int begin) {
  if(dest == nullptr) {
    return ;
  }
//  int copy_begin_index = GetSize() / 2;
  int move_size = GetSize() - begin;
  if(move_size <= 0) {
    return ;
  }
  // No delete but decrease size
  dest->SplitAndRcv(array_ + begin, move_size);
  IncreaseSize(-move_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SplitAndRcv(MappingType *items, int size) {
  // copy from items
  std::copy(items, items + size, array_ + GetSize());
  IncreaseSize(size);
}

//INDEX_TEMPLATE_ARGUMENTS
//void B_PLUS_TREE_LEAF_PAGE_TYPE::RisenKey(LeafPage *old_node, Transaction &transaction) {
//
//}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
