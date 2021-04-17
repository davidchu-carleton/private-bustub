//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <algorithm>

namespace bustub {

LRUReplacer::LRUReplacer() = default;

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    std::lock_guard<std::mutex> guardo(latch);
    if(!my_list.empty()) {
        *frame_id = my_list.front();
        Pin(*frame_id);
        return true;
    }
    *frame_id = -1;
    return false; 
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guardo(latch);
    my_list.remove(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guardo(latch);
    if (std::find(my_list.begin(), my_list.end(), frame_id) == my_list.end()) {
        my_list.push_back(frame_id);
    }
}

size_t LRUReplacer::Size() {
    std::lock_guard<std::mutex> guardo(latch); 
    return my_list.size(); 
}

}  // namespace bustub
