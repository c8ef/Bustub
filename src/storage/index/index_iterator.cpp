/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(bool is_end, BufferPoolManager *bpm, int curr_slot, const KeyComparator &comparator,
                                  Page *page)
    : is_end_{is_end}, bpm_{bpm}, curr_slot_{curr_slot}, comparator_{comparator}, page_{page} {
  std::cout << "init index interator!!\n";
  auto inner_data = reinterpret_cast<MappingType *>(page->GetData() + LEAF_PAGE_HEADER_SIZE);
  curr_ = &inner_data[curr_slot];
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() const -> bool { return is_end_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (is_end_) {
    throw std::runtime_error("can not deref end iterator");
  }
  return *curr_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  std::cout << "Usage index iterator!++\n";
  if (is_end_) {
    throw std::runtime_error("can not increment end iterator");
  }
  auto *leaf = reinterpret_cast<LeafPage *>(page_);
  auto inner_data = reinterpret_cast<MappingType *>(page_->GetData() + LEAF_PAGE_HEADER_SIZE);
  if (curr_slot_ < leaf->GetSize() - 1) {
    curr_slot_++;
    curr_ = &inner_data[curr_slot_];
    return *this;
  }
  if (leaf->GetNextPageId() == INVALID_PAGE_ID) {
    bpm_->UnpinPage(leaf->GetPageId(), false);
    is_end_ = true;
    return *this;
  }
  auto next_page = bpm_->FetchPage(leaf->GetNextPageId());
  bpm_->UnpinPage(leaf->GetPageId(), false);
  page_ = next_page;
  curr_slot_ = 0;
  auto next_inner_data = reinterpret_cast<MappingType *>(page_->GetData() + LEAF_PAGE_HEADER_SIZE);
  curr_ = &next_inner_data[curr_slot_];
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  if (this->IsEnd() && itr.IsEnd()) {
    return true;
  }
  if ((!this->IsEnd() && itr.IsEnd()) || (this->IsEnd() && !itr.IsEnd())) {
    return false;
  }
  if (comparator_(curr_->first, itr.curr_->first) == 0) {
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !this->operator==(itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
