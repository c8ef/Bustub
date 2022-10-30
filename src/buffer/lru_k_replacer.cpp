//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> l(latch_);
  if (curr_size_ == 0) {
    return false;
  }

  // find if there is item in the cache has less than k timestamps
  bool has_less_k = false;
  for (const auto &p : frame_map_) {
    if (p.second.timestamps_.size() < k_) {
      has_less_k = true;
      break;
    }
  }

  if (has_less_k) {
    size_t time_stamp = std::numeric_limits<size_t>::max();
    size_t id = 0;
    for (const auto &p : frame_map_) {
      if (p.second.timestamps_.size() < k_) {
        if (p.second.evictable_ && p.second.timestamps_.front() < time_stamp) {
          time_stamp = p.second.timestamps_.front();
          id = p.first;
        }
      }
    }

    // if those with less than k timestamps cannot evict, find evict element with k timestamps
    if (time_stamp == std::numeric_limits<size_t>::max()) {
      for (const auto &p : frame_map_) {
        if (p.second.evictable_ && p.second.timestamps_.front() < time_stamp) {
          time_stamp = p.second.timestamps_.front();
          id = p.first;
        }
      }
      // still cannot find, return error
      if (time_stamp == std::numeric_limits<size_t>::max()) {
        return false;
      }
      *frame_id = id;
      frame_map_.erase(*frame_id);
      curr_size_--;
      return true;
    }
    *frame_id = id;
    frame_map_.erase(*frame_id);
    curr_size_--;
    return true;
  }
  size_t time_stamp = std::numeric_limits<size_t>::max();

  size_t id = 0;
  for (const auto &p : frame_map_) {
    if (p.second.evictable_ && p.second.timestamps_.front() < time_stamp) {
      time_stamp = p.second.timestamps_.front();
      id = p.first;
    }
  }
  if (time_stamp == std::numeric_limits<size_t>::max()) {
    return false;
  }
  *frame_id = id;
  frame_map_.erase(*frame_id);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> l(latch_);
  if (frame_map_.count(frame_id) != 0) {
    frame_map_[frame_id].timestamps_.push_back(current_timestamp_);
    current_timestamp_++;
    if (frame_map_[frame_id].timestamps_.size() > k_) {
      frame_map_[frame_id].timestamps_.pop_front();
    }
  } else {
    FrameInfo info;
    info.timestamps_.push_back(current_timestamp_);
    current_timestamp_++;
    info.evictable_ = false;
    frame_map_[frame_id] = info;
    if (frame_map_.size() > replacer_size_) {
      frame_id_t evict;
      Evict(&evict);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> l(latch_);
  if (frame_map_.count(frame_id) != 0) {
    auto &info = frame_map_[frame_id];
    auto old_evict = info.evictable_;
    info.evictable_ = set_evictable;
    if (!old_evict && set_evictable) {
      curr_size_++;
    } else if (old_evict && !set_evictable) {
      curr_size_--;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> l(latch_);
  if (curr_size_ == 0 || frame_map_.count(frame_id) == 0 || !frame_map_[frame_id].evictable_) {
    return;
  }
  frame_map_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> l(latch_);
  return curr_size_;
}

}  // namespace bustub
