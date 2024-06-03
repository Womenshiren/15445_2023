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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if(curr_size_ == 0){
    return false;
  }
  for(auto it = node_list_.rbegin(); it!=node_list_.rend();it++){
    auto id = *it;
    if(evict_map_[id]){
      auto list_iter = node_store_[id];
      node_list_.erase(list_iter);
      node_store_.erase(id);
      evict_map_[id] = false;
      access_count_[id] = 0;
      *frame_id = id;
      curr_size_--;
      return true;
    }
  }
  for(auto it = buffer_list_.rbegin(); it!=buffer_list_.rend();it++){
    auto id = *it;
    if(evict_map_[id]){
      auto list_iter = buffer_store_[id];
      buffer_list_.erase(list_iter);
      buffer_store_.erase(id);
      evict_map_[id] = false;
      access_count_[id] = 0;
      *frame_id = id;
      curr_size_--;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  if(frame_id>static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  access_count_[frame_id]++;
  if(access_count_[frame_id] ==k_){
    //remove node from node_list_ to buffer_list_
    auto list_iter = node_store_[frame_id];
    node_list_.erase(list_iter);
    node_store_.erase(frame_id);
    buffer_list_.push_front(frame_id);
    buffer_store_[frame_id] = buffer_list_.begin();
  }
  else if(access_count_[frame_id] < k_){
    //earliest accesses are not changed, order in the buffer_list_ is not changed
    //if list is empty, add another item to it, else let it stay where it was
    if(node_store_.count(frame_id) == 0){
      node_list_.push_front(frame_id);
      node_store_[frame_id] = node_list_.begin();
    }
  }
  else{
    //remove the frame from buffer_list and push to the front
    auto list_iter = buffer_store_[frame_id];
    buffer_list_.erase(list_iter);
    buffer_list_.push_front(frame_id);
    buffer_store_[frame_id] = buffer_list_.begin();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (!evict_map_[frame_id] && set_evictable) {
    curr_size_++;
  }
  if (evict_map_[frame_id] && !set_evictable) {
    curr_size_--;
  }
  evict_map_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  size_t count = access_count_[frame_id];
  if(count == 0){
    return;
  }
  if (!evict_map_[frame_id]) {
    throw std::exception();
  }
  if(count < k_){
    auto list_iter = node_store_[frame_id];
    node_list_.erase(list_iter);
    node_store_.erase(frame_id);
  }
  else{
    auto list_iter = buffer_store_[frame_id];
    buffer_list_.erase(list_iter);
    buffer_store_.erase(frame_id);
  }
  evict_map_[frame_id] = false;
  access_count_[frame_id] = 0;
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
