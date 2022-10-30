//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  for (int i = 0; i < num_buckets_; ++i) {
    dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[std::hash<K>()(key) & ((1 << global_depth_) - 1)]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[std::hash<K>()(key) & ((1 << global_depth_) - 1)]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  InsertInner(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInner(const K &key, const V &value) {
  size_t dir_index = std::hash<K>()(key) & ((1 << global_depth_) - 1);
  if (dir_[dir_index]->Insert(key, value)) {
    return;
  }
  if (GetLocalDepthInternal(dir_index) < GetGlobalDepthInternal()) {
    auto bucket1 = std::make_shared<Bucket>(bucket_size_, GetLocalDepthInternal(dir_index) + 1);
    auto bucket2 = std::make_shared<Bucket>(bucket_size_, GetLocalDepthInternal(dir_index) + 1);
    num_buckets_++;
    size_t old_mask = std::hash<K>()(key) & ((1 << GetLocalDepthInternal(dir_index)) - 1);
    size_t mask = ((1 << GetLocalDepthInternal(dir_index)) - 1);
    auto split_list = dir_[dir_index]->GetItems();

    for (size_t i = 0; i < dir_.size(); ++i) {
      if ((i & mask) == old_mask) {
        if ((i & (mask + 1)) != 0U) {
          dir_[i] = bucket1;
        } else {
          dir_[i] = bucket2;
        }
      }
    }

    for (auto p = split_list.begin(); p != split_list.end(); ++p) {
      dir_[std::hash<K>()(p->first) & ((1 << global_depth_) - 1)]->Insert(p->first, p->second);
    }
    split_list.clear();
    if (dir_[dir_index]->Insert(key, value)) {
      return;
    }
    InsertInner(key, value);
  } else {
    auto bucket1 = std::make_shared<Bucket>(bucket_size_, GetLocalDepthInternal(dir_index) + 1);
    auto bucket2 = std::make_shared<Bucket>(bucket_size_, GetLocalDepthInternal(dir_index) + 1);
    num_buckets_++;
    size_t old_mask = std::hash<K>()(key) & ((1 << GetLocalDepthInternal(dir_index)) - 1);
    std::vector<std::shared_ptr<Bucket>> new_dir(dir_.size() * 2);
    auto split_list = dir_[dir_index]->GetItems();

    if (new_dir.size() != 2) {
      for (size_t i = 0; i < new_dir.size(); ++i) {
        if ((i & ((1 << GetLocalDepthInternal(dir_index)) - 1)) == old_mask) {
          if ((i & (1 << GetLocalDepthInternal(dir_index)))) {
            new_dir[i] = bucket1;
          } else {
            new_dir[i] = bucket2;
          }
        } else {
          new_dir[i] = dir_[i & ((1 << global_depth_) - 1)];
        }
      }
    } else {
      new_dir[0] = bucket1;
      new_dir[1] = bucket2;
    }
    global_depth_++;
    for (const auto &p : split_list) {
      new_dir[std::hash<K>()(p.first) & ((1 << global_depth_) - 1)]->Insert(p.first, p.second);
    }

    split_list.clear();
    if (new_dir[std::hash<K>()(key) & ((1 << global_depth_) - 1)]->Insert(key, value)) {
      dir_ = new_dir;
      return;
    }
    dir_ = new_dir;
    InsertInner(key, value);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  for (const auto &p : list_) {
    if (p.first == key) {
      value = p.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (GetSize() == 0) {
    return false;
  }
  auto iter = list_.begin();
  for (; iter != list_.end(); ++iter) {
    if (iter->first == key) {
      break;
    }
  }
  if (iter->first == key) {
    list_.erase(iter);
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  for (auto &p : list_) {
    if (p.first == key) {
      p.second = value;
      return true;
    }
  }
  if (IsFull()) {
    return false;
  }
  list_.push_back({key, value});
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
