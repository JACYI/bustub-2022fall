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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : capacity_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  // When number of evictable frame is zero
  if (Size() == 0) {
    return false;
  }

  // Always try to evict the frame with +inf k-instance first
  //    if(cache_list_.size() > 0) ;
  for(auto itr = cache_list_.rbegin(); itr != cache_list_.rend(); itr++) {
    auto node = *itr;
    if(node.is_evictable_){
      *frame_id = node.fid_;
      cache_node_.erase(node.fid_);
      cache_list_.erase(std::next(itr).base());
      curr_size_--;
      replacer_size_--;
      return true;
    }
  }

  // If not found, then evict the hot data
  for(auto itr = lru_k_list_.rbegin(); itr != lru_k_list_.rend(); itr++) {
    auto node = *itr;
    if(node.is_evictable_) {
      *frame_id = node.fid_;
      node_store_.erase(node.fid_);
      lru_k_list_.erase(std::next(itr).base());
      curr_size_--;
      replacer_size_--;
      return true;
    }

  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(frame_id != INVALID_PAGE_ID, "Frame is valid");
  std::lock_guard<std::mutex> lock(latch_);
  // Hot data
  if(node_store_.count(frame_id) != 0) {
    // Adjust to listHead
    auto node = *node_store_[frame_id];
    lru_k_list_.erase(node_store_[frame_id]);
    node.history_->push_front(current_timestamp_++);
    node.history_->pop_back();
    node.k_ = node.history_->back();
    lru_k_list_.push_front(node);
    node_store_[frame_id] = lru_k_list_.begin();
    return;
  }

  // Never seen before, initialize the LRUKNode
  if(cache_node_.count(frame_id) == 0) {
    while(curr_size_ >= capacity_) {
      // cache is full, need to evict
      cache_node_.erase(cache_list_.front().fid_);
      cache_list_.pop_back();
      curr_size_--;
    }
    std::unique_ptr<LRUKNode> new_node(new LRUKNode());
    new_node->fid_ = frame_id;
    new_node->k_ = INT_MAX;
    new_node->history_->push_front(current_timestamp_++);
    cache_list_.push_front(std::move(*new_node));
    cache_node_[frame_id] = cache_list_.begin();
    curr_size_++;

  } else {
    // Record timestamp
    auto node = *cache_node_[frame_id];
    cache_list_.erase(cache_node_[frame_id]);
    node.history_->push_front(current_timestamp_++);
    cache_list_.push_front(node);
    cache_node_[frame_id] = cache_list_.begin();

  }

  //  while(history_list.size() > k_) {
  //    history_list.pop_front();
  //  }

  // Reach to the thresh of hot data
  if(cache_node_[frame_id]->history_->size() == k_) {
    auto his_node = *cache_node_[frame_id];
    // Delete from cache list
    cache_list_.erase(cache_node_[frame_id]);
    cache_node_.erase(frame_id);
    // And add it to hot list
    his_node.k_ = his_node.history_->back();
    lru_k_list_.push_front(his_node);
    node_store_[frame_id] = lru_k_list_.begin();

  }

}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if(node_store_.count(frame_id) != 0) {
    replacer_size_ += static_cast<int>(set_evictable) -
                      static_cast<int>(node_store_[frame_id]->is_evictable_);
    node_store_[frame_id]->is_evictable_ = set_evictable;
  } else if (cache_node_.count(frame_id) != 0) {
    replacer_size_ += static_cast<int>(set_evictable) -
                      static_cast<int>(cache_node_[frame_id]->is_evictable_);
    cache_node_[frame_id]->is_evictable_ = set_evictable;
  } else {
    std::cout<<"Invalid Page ID"<<std::endl;
    throw std::exception();
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if(node_store_.count(frame_id) != 0) {
    auto node = node_store_[frame_id];
    if(!node->is_evictable_) {
      // Non-evictable
      throw std::exception();
    }
    lru_k_list_.erase(node);
    node_store_.erase(frame_id);
    replacer_size_--;
    curr_size_--;

  } else if(cache_node_.count(frame_id) != 0) {
    auto node = cache_node_[frame_id];
    if(!node->is_evictable_) {
      throw std::exception();
    }
    cache_list_.erase(node);
    cache_node_.erase(frame_id);
    replacer_size_--;
    curr_size_--;

  }
//  else {
//    throw std::exception();
//  }

}

// Return the number of evictable frames
auto LRUKReplacer::Size() -> size_t {
  //  std::lock_guard<std::mutex> lock(latch_);
  return replacer_size_;
}

}  // namespace bustub
