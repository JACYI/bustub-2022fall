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
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  latch_.lock();
  frame_id_t free_frame;

  if(!free_list_.empty()) {
    // Get free frame_id_t
    free_frame = free_list_.front();
    free_list_.pop_front();

  } else {
    // Not exists free page, replacer victim a LRU page
    if (!replacer_->Evict(&free_frame)) {
      // No frames can be evicted.
      latch_.unlock();
      return nullptr;
    }

    // If page is dirty, then write it to the disk first
    if (pages_[free_frame].is_dirty_) {
      disk_manager_->WritePage(pages_[free_frame].page_id_,
                               pages_[free_frame].data_);
      pages_[free_frame].is_dirty_ = false;
    }

    // Reset the memory and the metadata
    //    auto rpc_page = pages_[free_frame];
    page_table_.erase(pages_[free_frame].page_id_);
    pages_[free_frame].ResetMemory();
    pages_[free_frame].page_id_ = INVALID_PAGE_ID;
    pages_[free_frame].pin_count_ = 0;
  }

  page_id_t new_page_id = AllocatePage();
  // Add new page_id to pages
  pages_[free_frame].page_id_ = new_page_id;
  pages_[free_frame].pin_count_ = 1;   // At least one thread call this func

  // Establish the relation between page and frame in page table
  page_table_[new_page_id] = free_frame;
  // Pin the frame
  replacer_->RecordAccess(free_frame);
  replacer_->SetEvictable(free_frame, false);
  *page_id = new_page_id;
  latch_.unlock();

  return &pages_[free_frame];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  latch_.lock();
  Page * target_page;
  // Search for page_id in the buffer pool
  if(page_table_.count(page_id) != 0) {
    auto target_frame = page_table_[page_id];
    target_page = &pages_[target_frame];
    target_page->pin_count_++;
    replacer_->RecordAccess(target_frame);
    latch_.unlock();
    return target_page;
  }

  // If not found,
  // ----------------- pick a replacement frame ---------------------
  frame_id_t rpc_frame;
  if(!free_list_.empty()) {
    rpc_frame = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&rpc_frame)) {
      latch_.unlock();
      return nullptr;
    }
    if (pages_[rpc_frame].is_dirty_) {
      // Write to disk
      disk_manager_->WritePage(pages_[rpc_frame].page_id_, pages_[rpc_frame].data_);
      pages_[rpc_frame].is_dirty_ = false;
    }
    pages_[rpc_frame].ResetMemory();
    pages_[rpc_frame].page_id_ = INVALID_PAGE_ID;
    pages_[rpc_frame].is_dirty_ = false;
    pages_[rpc_frame].pin_count_ = 0;
    page_table_.erase(rpc_frame);
  }
  pages_[rpc_frame].page_id_ = page_id;
  pages_[rpc_frame].pin_count_ = 1;
  page_table_[page_id] = rpc_frame;
  replacer_->RecordAccess(rpc_frame);
  replacer_->SetEvictable(rpc_frame, false);
  // ------------------------------------------------------------

  // Read the page from the disk
  disk_manager_->ReadPage(page_id, pages_[rpc_frame].data_);
  latch_.unlock();
  return &pages_[rpc_frame];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {latch_.lock();
  // Page not found
  if(page_table_.count(page_id) == 0) {
    latch_.unlock();
    return false;
  }
  // pin_count less or equal than zero
  auto unpin_frame = page_table_[page_id];
  if(pages_[unpin_frame].pin_count_ <= 0) {
    latch_.unlock();
    return false;
  }

  // Decrease the pin_count
  pages_[unpin_frame].pin_count_--;
  if(pages_[unpin_frame].pin_count_ == 0) {
    // Firstly decrease to zero, should be set for evictable
    replacer_->SetEvictable(unpin_frame, true);
  }
  // Set dirty flag
  if(is_dirty) {
    pages_[unpin_frame].is_dirty_ = true;
  }

  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  latch_.lock();
  if(page_table_.count(page_id) == 0) {
    // Page not found
    latch_.unlock();
    return false;
  }
  // Flush to disk
  auto target_frame = page_table_[page_id];
  if(pages_[target_frame].page_id_ == INVALID_PAGE_ID) {
    // Invalid page
    latch_.unlock();
    return false;
  }
  // Write to disk
  disk_manager_->WritePage(page_id, pages_[target_frame].data_);
  pages_[target_frame].is_dirty_ = false;
  latch_.unlock();
  return true;

}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  latch_.lock();
  // Flush all the pages in buffer pool
  for(int i=0; i<static_cast<int>(pool_size_); i++) {
    FlushPage(pages_[i].page_id_);
  }
  latch_.unlock();
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  latch_.lock();
  if(page_table_.count(page_id) == 0){
    latch_.unlock();
    return true;
  }
  auto delete_frame = page_table_[page_id];
  if(pages_[delete_frame].pin_count_ > 0) {
    // Pinned
    latch_.unlock();
    return false;
  }

  // Stop tracking and add delete frame to free list
  replacer_->Remove(delete_frame);
  page_table_.erase(page_id);
  free_list_.push_back(delete_frame);
  // Reset the data
  pages_[delete_frame].page_id_ = INVALID_PAGE_ID;
  pages_[delete_frame].ResetMemory();
  pages_[delete_frame].pin_count_ = 0;
  pages_[delete_frame].is_dirty_ = false;

  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
