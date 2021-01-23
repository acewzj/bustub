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

LRUReplacer::LRUReplacer(size_t num_pages) : capacity_(num_pages) {
    head_ = new LinkNode();
    tail_ = new LinkNode();
    head_->next = tail_;
    tail_->prev = head_;
}

LRUReplacer::~LRUReplacer() {
    delete head_;
    delete tail_;
};

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    std::lock_guard<std::mutex> lock(mutex_);
    if (size_ == 0) {
        return false;
    }
    //找到最后一个节点
    LinkNode* node = tail_->prev;
    //删除掉它
    node->prev->next = tail_;
    tail_->prev = node->prev;
    *frame_id = node->data;
    map_cache.erase(node->data);
    delete node;
    --size_;    
    return true;    
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    //如果没有这个值就返回
    if (map_cache.count(frame_id) == 0) {
        return;
    }else{
        LinkNode* node = map_cache[frame_id];
        map_cache.erase(node->data);
        node->prev->next = node->next;
        node->next->prev = node->prev;
        --size_;
        delete node;
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    //如果没有这个值就插进去
    if (map_cache.count(frame_id) == 0) {
        LinkNode* node = new LinkNode();
        node->data = frame_id;
        ++size_;
        map_cache.insert({frame_id, node});
        head_->next->prev = node;
        node->next = head_->next;
        node->prev = head_;
        head_->next = node;
        if (size_ > capacity_) {
            //找到最后一个节点
            LinkNode* node = tail_->prev;
            //删除掉它
            node->prev->next = tail_;
            tail_->prev = node->prev;   
            delete node;
            --size_;         
        }
    }else{
        return;
    }
}

size_t LRUReplacer::Size() {
    std::lock_guard<std::mutex> lock(this->mutex_);
     return size_;
}

}  // namespace bustub
