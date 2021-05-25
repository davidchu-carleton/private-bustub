//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Method to find the first index i so that array[i].first >= key
 * NOTE: This method is primarily useful when constructing an index iterator
 *       that begins at a certain key.
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (comparator(array[i].first, key) >= 0) {
      return i;
    }
  }
  return GetSize() - 1;
}

/*
 * Find and return the key stored at "index"
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code (fill in the curly braces with the appropriate key value)
  KeyType key{array[index].first};
  return key;
}

/*
 * Find and return the key & value pair stored at "index"
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array[index];
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  int size = GetSize();
  if (size == 0 || comparator(key, KeyAt(0)) < 0 || comparator(key, KeyAt(size - 1)) > 0) {
    return false;
  }
  int key_index = KeyIndex(key, comparator);
  if (comparator(array[key_index].first, key) == 0) {
    *value = array[key_index].second;
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  // Do not need to split here
  int size = GetSize();
  MappingType new_pair = std::make_pair(key, value);
  // bigger than the last one
  if (size == 0 || comparator(key, array[size - 1].first) > 0) {
    array[size] = new_pair;
  } else if (comparator(key, array[0].first) < 0) {  // less than the first one
    //memmove(array + 1, array, static_cast<size_t>(GetSize() * sizeof(MappingType)));
    for (int i = size + 1; i >= 1; i--) {
      array[i] = array[i - 1];
    }
    array[0] = new_pair;
  } else {
    // in the middle of array
    int new_index = KeyIndex(key, comparator);
    //memmove(array + new_index + 1, array + new_index, static_cast<size_t>((GetSize() - new_index) * sizeof(MappingType)));
    for (int i = size + 1; i >= new_index + 1; i--) {
      array[i] = array[i - 1];
    }
    array[new_index] = new_pair;
  }
  IncreaseSize(1);
  return size + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * Note: you might find it useful to assume recipient is a new, empty page
 *       and call MoveHalfTo accordingly in b_plus_tree.cpp.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int half = (GetSize() + 1) / 2;
  recipient->CopyNFrom(array + GetSize() - half, half);
  IncreaseSize(-1 * half);
}

/*
 * Private helper method for MoveHalfTo.
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  memcpy(array, items, size * sizeof(MappingType));
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: shift entries over to fill in removed slot
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) {
  int size = GetSize();
  if (size == 0 || comparator(key, KeyAt(0)) < 0 || comparator(key, KeyAt(size - 1)) > 0) {
    return size;
  }

  int key_index = KeyIndex(key, comparator);
  if (comparator(key, KeyAt(key_index)) == 0) {
    //memmove(array + key_index, array + key_index + 1, (size - key_index - 1) * sizeof(MappingType));
    for (int i = key_index; i < size - 1; i++) {
      array[i] = array[i + 1];
    }
    IncreaseSize(-1);
    size--;
  }

  return size;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  recipient->CopyAllFrom(array, GetSize());
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
}

/*
 * Copy all key & value pairs from items to me
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {
  memcpy(array + GetSize(), items, size * sizeof(MappingType));
  IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  int size = GetSize();
  recipient->CopyLastFrom(array[0]);
  //memmove(array, array + 1, (GetSize() - 1) * sizeof(MappingType));
  for (int i = 0; i < size - 1; i++) {
      array[i] = array[i + 1];
  }
  IncreaseSize(-1);
  // ask Aaron about buffer_poop_manager
  // auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  // auto parent_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
  // parent_node->SetKeyAt(parent_node->ValueIndex(GetPageId()), array[0].first);
  // buffer_pool_manager->UnpinPage(parent_node->GetPageId(), true);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array[GetSize()] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  recipient->CopyFirstFrom(array[GetSize() - 1]);
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  memmove(array + 1, array, GetSize() * sizeof(MappingType));
  array[0] = item;
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
