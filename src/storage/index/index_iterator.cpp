/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index, BufferPoolManager *buffer_pool_manager)
    : page_id_(page_id), index_(index), buffer_pool_manager_(buffer_pool_manager) {
  auto leaf_page = buffer_pool_manager_->FetchPage(page_id_);
  leaf_node_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() { buffer_pool_manager_->UnpinPage(page_id_, false); }

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  auto next_id = leaf_node_->GetNextPageId();
  return (leaf_node_ == nullptr || (index_ >= leaf_node_->GetSize() && next_id == INVALID_PAGE_ID));
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  bool res = (index_ == itr.index_) && (page_id_ == itr.page_id_);
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  bool res = (index_ != itr.index_) || (page_id_ != itr.page_id_);
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  if (isEnd()) {
    throw std::out_of_range("Index_Iterator : out of range");
  }
  return leaf_node_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  index_++;
  if (index_ >= leaf_node_->GetSize()) {
    page_id_t next = leaf_node_->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf_node_->GetPageId(), false);
    if (next == INVALID_PAGE_ID) {
      // set index to a constant
      index_ = INT_MAX;
      page_id_ = INT_MAX;
      leaf_node_ = nullptr;
    } else {
      Page *page = buffer_pool_manager_->FetchPage(next);
      leaf_node_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
      page_id_ = next;
      index_ = 0;
    }
  }
  // if (next_page_id != INVALID_PAGE_ID) {
  // buffer_pool_manager_->UnpinPage(page_id_, false);
  // auto page = buffer_pool_manager_->FetchPage(next_page_id);
  // leaf_node_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  // index_ = 0;
  // page_id_ = next_page_id;
  //}
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
