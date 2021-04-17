//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>
#include <algorithm>
#include <vector>
#include <mutex>



namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  
  //std::lock_guard<std::mutex> guardo(latch);
  
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer();

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  std::lock_guard<std::mutex> guardo(latch);
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  Page *page = nullptr;
  frame_id_t frame_id;
  if (page_table_.count(page_id) > 0) {
    frame_id = page_table_.at(page_id);
    replacer_->Pin(frame_id);
    page = &pages_[frame_id];
    page->pin_count_++;
    return page;
  } 

  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    page = &pages_[frame_id];
  } else if (replacer_->Victim(&frame_id)){
    page = &pages_[frame_id];
  } else {
    //    If no page can be replaced, return nullptr
    return page;
  }

  // 2.     If R is dirty, write it back to the disk.
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->data_);
  }

  // 3.     Delete R from the page table and insert P.
  page_table_.erase(page->GetPageId());
  page_table_[page_id] = frame_id;

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P. 
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->page_id_ = page_id;
  disk_manager_->ReadPage(page_id, page->data_);
  return page;
}


bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guardo(latch);
  frame_id_t frame_id = page_table_.at(page_id);
  Page *page = &pages_[frame_id];
  if (page->pin_count_ <= 0) {
    return false;
  }
  page->pin_count_--;
  page->is_dirty_=is_dirty;
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  } 
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::lock_guard<std::mutex> guardo(latch);
  // Make sure you call DiskManager::WritePage!
  if (page_table_.count(page_id) > 0) {
    frame_id_t frame_id = page_table_.at(page_id);
    Page *page = &pages_[frame_id]; 
    disk_manager_->WritePage(page_id, page->data_);
    return true;
  } else {
    return false;
  }
}

Page *BufferPoolManager::NewPage(page_id_t *page_id) {
  std::lock_guard<std::mutex> guardo(latch);
  // 0.   Make sure you call DiskManager::AllocatePage!
  *page_id = disk_manager_->AllocatePage();

  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  Page *page;
  for(auto kv : page_table_) {
    page = &pages_[kv.second];
    if (page->pin_count_ > 0) {
      return nullptr;
    }
  }
  
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    page = &pages_[frame_id];
  } else if (replacer_->Victim(&frame_id)){
    page = &pages_[frame_id];
  } 

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  page = &pages_[frame_id];
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page_table_.erase(*page_id); // necessary?
  page->ResetMemory(); // zero out memory??
  page_table_[*page_id] = frame_id;

  // what if there is no frame_id?
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return page;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  std::lock_guard<std::mutex> guardo(latch);
  // 0.   Make sure you call DiskManager::DeallocatePage!
  disk_manager_->DeallocatePage(page_id);

  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  if (page_table_.count(page_id) == 0) {
    return true;
  }

  // if (page_table_.find(page_id) == page_table_.end()) {
  //    return true;
  // }

  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  frame_id_t frame_id = page_table_.at(page_id);
  Page *page = &pages_[frame_id];
  if (page->pin_count_ != 0) {
    return false;
  }
  
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  page_table_.erase(page_id);
  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  free_list_.push_back(frame_id);
  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> guardo(latch);
  //not sure?
  page_id_t page_id;
  for(auto kv : page_table_) {
    page_id = kv.first;
    FlushPage(page_id);
  }
}

}  // namespace bustub
