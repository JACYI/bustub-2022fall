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
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  auto itr = std::lower_bound(array_ + 1, array_ + GetSize(), key,
                              [&comparator](const auto &pair, auto k) {return comparator(pair.first, k) < 0; });
  return std::distance(array_, itr);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  auto it = std::find_if(array_, array_ + GetSize(),
                         [&value](const auto &pair) { return pair.second == value; });
  return std::distance(array_, it);
}

// find the target value with target key larger than or equal key
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key,
                                 [&comparator](const auto &pair, auto k) { return comparator(pair.first, k) < 0; });
  if (target == array_ + GetSize()) {
    return ValueAt(GetSize() - 1);
  }
  if (comparator(target->first, key) == 0) {
    return target->second;
  }
  return std::prev(target)->second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value) {

}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &comparator) -> int {
  int insert_index = KeyIndex(key, comparator);
  if(comparator(array_[insert_index].first, key) == 0){
    // Duplicate key
    return GetSize();
  }
  int cur_size = GetSize();
  if(insert_index < cur_size){
    std::move_backward(array_ + insert_index, array_ + cur_size, array_ + cur_size + 1);
  }

  array_[insert_index].first = key;
  array_[insert_index].second = value;
  IncreaseSize(1);

  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAfterNode(const ValueType &old_page_id, const KeyType &new_key, const ValueType &new_value) -> int {
  // new value position
  int offset = ValueIndex(old_page_id) + 1;
  if(offset < GetSize()){
    // insert after move elements behind
    std::copy(array_ + offset, array_ + GetSize(), array_ + offset + 1);
  }
  // Insert
  array_[offset].first = new_key;
  array_[offset].second = new_value;
  IncreaseSize(1);

  return GetSize();
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SplitAndSnd(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *dest, int begin) {
  if(dest == nullptr) {
    return ;
  }
  int copy_size = GetSize() - begin;
  // No delete but decrease size
  dest->SplitAndRcv(array_ + begin, copy_size);
  IncreaseSize(-copy_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SplitAndRcv(MappingType *items, int size) {
  // copy from items
  std::copy(items, items + size, array_ + GetSize());
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetItem(int index) -> const MappingType & {
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndDeleteRecord(int index) -> bool {
  int size = GetSize();
  if(index < 0 || index >= size) {
    return false;
  }
  if(index < size - 1) {
    std::move(array_ + index + 1, array_ + size, array_ + index);
  }
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndDeleteRecord(KeyType &key, KeyComparator &comparator) -> bool {
  int key_index = KeyIndex(key, comparator);
  if(key_index >= GetSize() || comparator(array_[key_index].first, key) != 0) {
    return false;
  }

  return RemoveAndDeleteRecord(key_index);
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
