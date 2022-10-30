//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> l{latch_};
  frame_id_t frame_id = FindEvict();
  if (frame_id == -1) {
    return nullptr;
  }
  pages_[frame_id].page_id_ = AllocatePage();
  *page_id = pages_[frame_id].page_id_;
  page_table_->Insert(pages_[frame_id].GetPageId(), frame_id);
  pages_[frame_id].ResetMemory();
  // pin_count add 1, not = 1
  pages_[frame_id].pin_count_++;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> l{latch_};
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  frame_id = FindEvict();
  if (frame_id == -1) {
    return nullptr;
  }

  pages_[frame_id].page_id_ = page_id;
  page_table_->Insert(page_id, frame_id);
  // pin_count add 1, not = 1
  pages_[frame_id].pin_count_++;
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> l{latch_};
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].GetPinCount() == 0) {
    return false;
  }

  // UnpinPage can modify the dirty bit only to mark it as dirty
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  pages_[frame_id].pin_count_--;

  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> l{latch_};
  frame_id_t frame;
  if (!page_table_->Find(page_id, frame)) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[frame].GetData());
  pages_[frame].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].GetPageId() != INVALID_PAGE_ID) {
      FlushPgImp(pages_[i].GetPageId());
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> l{latch_};
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  }

  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_front(frame_id);
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].ResetMemory();
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
