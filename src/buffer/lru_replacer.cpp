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

namespace bustub {

LRUReplacer::LRUReplacer() = default;

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    if(!my_list.empty()) {
        *frame_id = my_list.begin();
        Pin(frame_id);
        return true;
    }
    *frame_id = -1;
    return false; 
}

void LRUReplacer::Pin(frame_id_t frame_id) {

}

void LRUReplacer::Unpin(frame_id_t frame_id) {

}

size_t LRUReplacer::Size() { return my_list.size(); }

}  // namespace bustub
