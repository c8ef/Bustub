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

#include <algorithm>
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  auto index = std::find_if(array_, array_ + GetSize(), [&value](auto &&pair) { return pair.second == value; });
  return std::distance(array_, index);
}

// return page_id by input key
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookUp(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  auto index = std::lower_bound(array_ + 1, array_ + GetSize(), key,
                                [&comparator](auto &&pair, auto &&key) { return comparator(pair.first, key) < 0; });
  if (index == array_ + GetSize()) {
    return ValueAt(GetSize() - 1);
  }
  if (comparator(index->first, key) == 0) {
    return index->second;
  }
  return std::prev(index)->second;
}

// only leaf page turn into two leaf page & a root
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
  SetSize(2);
}

// insert a new pair after the pair with the old_value
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) -> int {
  auto index = ValueIndex(old_value) + 1;
  std::move_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);

  array_[index].first = new_key;
  array_[index].second = new_value;
  IncreaseSize(1);
  return GetSize();
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
}

// remove the only key value pair and return the value
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> ValueType {
  SetSize(0);
  return ValueAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *target, const KeyType &middle_key,
                                               BufferPoolManager *bpm) {
  SetKeyAt(0, middle_key);
  target->CopyNFrom(array_, GetSize(), bpm);
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *target, BufferPoolManager *bpm) {
  auto index = GetMinSize();
  SetSize(index);
  target->CopyNFrom(array_ + index, GetMaxSize() - index, bpm);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *target, const KeyType &middle_key,
                                                      BufferPoolManager *bpm) {
  SetKeyAt(0, middle_key);
  auto item = array_[0];
  target->CopyLastFrom(item, bpm);
  std::move(array_ + 1, array_ + GetSize(), array_);
  IncreaseSize(-1);
}

// middle key is like a push up search key
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *target, const KeyType &middle_key,
                                                       BufferPoolManager *bpm) {
  auto item = array_[GetSize() - 1];
  target->SetKeyAt(0, middle_key);
  target->CopyFirstFrom(item, bpm);
  IncreaseSize(-1);
}

// copy also handles parent id change
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *bpm) {
  std::copy(items, items + size, array_ + GetSize());

  for (int i = 0; i < size; ++i) {
    auto page = bpm->FetchPage(ValueAt(i + GetSize()));
    if (page == nullptr) {
      throw std::runtime_error{"cannot allocate page from buffer pool"};
    }
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    node->SetParentPageId(GetPageId());
    bpm->UnpinPage(page->GetPageId(), true);
  }
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *bpm) {
  *(array_ + GetSize()) = pair;
  IncreaseSize(1);

  auto page = bpm->FetchPage(pair.second);
  if (page == nullptr) {
    throw std::runtime_error{"cannot allocate page from buffer pool"};
  }
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(GetPageId());
  bpm->UnpinPage(page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *bpm) {
  std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
  *array_ = pair;
  IncreaseSize(1);

  auto page = bpm->FetchPage(pair.second);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(GetPageId());
  bpm->UnpinPage(page->GetPageId(), true);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
