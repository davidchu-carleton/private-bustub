//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page.
 * Including set page type, set current size, set page id, set parent id and set
 * max page size.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}

/*
 * Get the key stored at index.
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code (fill in the curly braces with the appropriate key value)
  KeyType key{
    array[index].first
  };
  return key;
}

/*
 * Set the key stored at index.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array[index].first = key;
}

/*
 * Find and return array index so that its value equals to input "value".
 * Return -1 if the value is not found.
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const { 
  for (int i = 0; i < GetSize(); i++) {
    if (array[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Get the value stored at index.
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { 
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that would contain input "key".
 * Start the search from the second key (the first key should always be invalid).
 * Remember that page_id at index i refers to a subtree in which all keys K satisfy:
 * K(i) < K <= K(i+1).
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  if(comparator(key, array[1].first) < 0){
    return array[0].second;
  } else if(comparator(key, array[GetSize()-1].first) > 0){
    return array[GetSize()-1].second;
  }

  for(int i = 1; i < GetSize(); i++){
    if(comparator(key, array[i].first) < 0){
      return array[i-1].second;
    }
  }

  return INVALID_PAGE_ID;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way up to the root
 * page, you will create a new root page and call this method to populate its elements.
 * NOTE: This method is only called on a new root node from InsertIntoParent()(b_plus_tree.cpp)
 * where old_value is page_id of old root, new_value is page_id of new split
 * page (successor sibling to old root) and new_key is the middle key.
 * NOTE: Make sure to update the size of this node.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array[0].second = old_value;
  array[1] = std::make_pair(new_key, new_value);
  IncreaseSize(1);
}
/*
 * Insert new_key & new_value pair right after the pair with its value == old_value
 * You can assume old_value will be present in this node.
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int size = GetSize();
  int old_index = ValueIndex(old_value);
  memmove(array + old_index + 2, array + old_index + 1, 
  (size - old_index - 1) * sizeof(MappingType));
  array[old_index + 1] = std::make_pair(new_key, new_value);
  IncreaseSize(1);
  return size + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page.
 * Note: you might find it useful to assume recipient is a new, empty page
 *       and call MoveHalfTo accordingly in b_plus_tree.cpp.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int half = (GetSize() + 1)/2;
  recipient->CopyNFrom(array + GetSize() - half, half, buffer_pool_manager);
  IncreaseSize(-1 * half);
}

/*
 * Private helper method for MoveHalfTo.
 * Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 * (i.e., fetch each child page, update the parent page id, and unpin as dirty).
 *
 * To call this from MoveHalfTo, use array + offset to get a pointer to where you want to
 * start copying (the items parameter). offset would be the middle index.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  memcpy(array, items, size * sizeof(MappingType));
  for(int i = 0; i < size; i++) {
    auto child_page = buffer_pool_manager->FetchPage(array[i].second);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData()); 
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_node->GetPageId(), true);
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page stored at index
 * NOTE: shift entries over to fill in removed slot
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) { 
  memmove(array + index, array + index + 1, (GetSize()-index-1) * sizeof(MappingType));
  IncreaseSize(-1);
  return GetSize();
 }

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient. Remember to adjust node sizes.
 * Note: you might find it helpful to assume recipient is a predecessor of this
 *       node (i.e, insert middle key at the end of it, followed by everything from this node)
 *       and then call MoveAllTo accordingly in b_plus_tree.cpp.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  // Update parent page
  auto parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
  auto parent_node = reinterpret_cast<BPlusTreeInternalPage *>(parent_page->GetData());
  // Change key in the parent page
  SetKeyAt(0, middle_key);
  buffer_pool_manager->UnpinPage(parent_node->GetPageId(), false); 
  recipient->CopyNFrom(array + size, size, buffer_pool_manager); 
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient. Remember to adjust node sizes.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  //Construct the pair
  auto parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
  auto parent_node = reinterpret_cast<BPlusTreeInternalPage *>(parent_page->GetData());
  int parent_index = parent_node->ValueIndex(GetPageId());
  MappingType first_pair = std::make_pair(middle_key, array[0].second);
  recipient->CopyLastFrom(first_pair, buffer_pool_manager);
  //Change parent page
  parent_node->SetKeyAt(parent_index, array[1].first);
  Remove(0); 
  //Unpin pages
  buffer_pool_manager->UnpinPage(parent_node->GetPageId(), true);
}

/*
 * Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  array[size] = pair;
  IncreaseSize(1);
  // Change metadata in child page
  auto child_page = buffer_pool_manager->FetchPage(pair.second);
  auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager, const int parent_index) {
  int size = GetSize();
  recipient->CopyFirstFrom(array[size-1], buffer_pool_manager, middle_key, parent_index);
  IncreaseSize(-1);                                                       
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager, 
                                                    const KeyType &middle_key, const int parent_index) {
  int size = GetSize();

  //Insert into the first position
  memmove(array + 1, array, size * sizeof(MappingType));
  array[0] = pair;

  //Change parent page
  auto parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
  auto parent_node = reinterpret_cast<BPlusTreeInternalPage *>(parent_page->GetData());
  array[0].first = middle_key;
  parent_node->SetKeyAt(parent_index, pair.first);

  //Change child page
  parent_page = buffer_pool_manager->FetchPage(pair.second);
  auto child_page = reinterpret_cast<BPlusTreePage *>(parent_page->GetData());
  child_page->SetParentPageId(GetPageId());

  //unpin pages
  buffer_pool_manager->UnpinPage(parent_node->GetPageId(), true);
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
  IncreaseSize(1); 
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
