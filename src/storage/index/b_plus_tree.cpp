//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <cinttypes>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"
#include "common/logger.h"



namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * @return true if there is nothing stored in the b+ tree, false otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Add the value that is associated with parameter key to the vector result
 * if key exists.
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  if(IsEmpty()) {
    return false;
  }
  auto leaf_page = FindLeafPage(key, false);
  //auto leaf_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  ValueType temp_value;
  if (leaf_node->Lookup(key, &temp_value, comparator_)){
    LOG_INFO("# sucessful");
    result->push_back(temp_value);
    return true;
  }else{
    LOG_INFO("# failed");
    return false;
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user tries to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) { 
  if(IsEmpty()){
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * You should first ask for new page from buffer pool manager (NOTICE: throw
 * an std::bad_alloc exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto new_page = buffer_pool_manager_->NewPage(&root_page_id_);
  if(new_page == nullptr){
    throw std::bad_alloc();
  }
  auto new_page_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(new_page->GetData());
  new_page_node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  new_page_node->Insert(key, value, comparator_);
  UpdateRootPageId(true);
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * You should first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exists or not. If it exists, return
 * immdiately, otherwise insert entry. Remember to deal with a split if necessary.
 * @return: since we only support unique keys, if user tries to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  auto leaf_page = FindLeafPage(key, false);
  //auto leaf_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  // check if key exists
  ValueType temp_value;
  if(leaf_node->Lookup(key, &temp_value, comparator_)){
    return false;
  } else {
    leaf_node->Insert(key, value, comparator_);
    if(leaf_node->GetSize() > leaf_node->GetMaxSize()) {
      // LOG_INFO("# This is max size: %d", leaf_node->GetMaxSize());
      auto new_page_node = Split(leaf_node);
      // if (new_page_node->IsLeafPage()) {
      //   LOG_INFO("# This is insert into leaf - leaf");
      //   LeafPage *leaf = reinterpret_cast<LeafPage *>(new_page_node);
      //   InsertIntoParent(leaf_node, leaf->KeyAt(0), leaf, transaction);  
      // } else {
      //   LOG_INFO("# This is insert into leaf - internal");
      //   InternalPage *internal = reinterpret_cast<InternalPage *>(new_page_node);
      //   //LOG_INFO("Insert into leaf %" PRId64, internal->KeyAt(1).ToInt64());
      //   InsertIntoParent(leaf_node, internal->KeyAt(1), internal, transaction);
      // }
      LeafPage *leaf = reinterpret_cast<LeafPage *>(new_page_node);
      InsertIntoParent(leaf_node, leaf->KeyAt(0), leaf, transaction); 
    }
  return true;
  }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager (NOTICE: throw
 * an std::bad_alloc exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::Split(BPlusTreePage *node) { 
  page_id_t new_page_id; // place holder
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if(new_page == nullptr){
     throw std::bad_alloc();
  }
  //auto new_node = reinterpret_cast<N *>(new_page->GetData());
  BPlusTreePage *new_node_page = reinterpret_cast<BPlusTreePage *>(new_page->GetData());
  if (node->IsLeafPage()) {
    // leaf page split
    LeafPage *leaf = reinterpret_cast<LeafPage *>(node);
    LeafPage *new_leaf = reinterpret_cast<LeafPage *>(new_node_page);
    new_leaf->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
    new_leaf->SetNextPageId(leaf->GetNextPageId());
    leaf->SetNextPageId(new_leaf->GetPageId());
    leaf->MoveHalfTo(new_leaf);
    return new_leaf;
    } else {
      // internal page split
    InternalPage *internal = reinterpret_cast<InternalPage *>(node);
    InternalPage *new_internal = reinterpret_cast<InternalPage *>(new_node_page);
    new_internal->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    internal->MoveHalfTo(new_internal, buffer_pool_manager_);
    return new_internal;
  }
 }

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * You first needs to find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to recursively
 * insert in parent if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
    if (old_node->IsRootPage()) {
      //LOG_INFO("# This is root split\n");
      //LOG_INFO("Root add key %" PRId64, key.ToInt64());
      auto newPage = buffer_pool_manager_->NewPage(&root_page_id_);
      InternalPage *newRoot = reinterpret_cast<InternalPage *>(newPage);
      newRoot->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
      newRoot->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
      old_node->SetParentPageId(root_page_id_);
      new_node->SetParentPageId(root_page_id_);
      UpdateRootPageId();
      //fetch page and new page need to unpin page
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(newRoot->GetPageId(), true);
      return;
    }
    page_id_t parentId = old_node->GetParentPageId();
    auto *page = buffer_pool_manager_->FetchPage(parentId);
    InternalPage *parent = reinterpret_cast<InternalPage *>(page);
    new_node->SetParentPageId(parentId);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    //insert new node after old node
    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    if (parent->GetSize() > parent->GetMaxSize()) {
      //begin /* Split Parent */
      //LOG_INFO("# This is not root split\n");
      auto *newParentPage = Split(parent); //new page need unpin
      InternalPage *new_parent = reinterpret_cast<InternalPage *>(newParentPage);
      InsertIntoParent(parent, new_parent->KeyAt(0), new_parent, transaction);
    }
    buffer_pool_manager_->UnpinPage(parentId, true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  }

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, you first need to find the right leaf page as deletion target, then
 * delete entry from leaf page.
 * Remember to call CoalesceOrRedistribute if necessary.
 * @param key                  the key to remove
 * @param transaction          the current Transaction object, use to record pages
 *                             you latch, and pages that should be deleted.
 *                             This method should unlatch and delete before returning.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*
 * You first need to find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, coalesce.
 * Using template N to represent either internal page or leaf page.
 * @param node                 the node that had a key removed
 * @param transaction          the current Transaction object, use to record pages for deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceOrRedistribute(BPlusTreePage *node, Transaction *transaction) {}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method CoalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @param   index              index of pointer to "node" within "parent"
 * @param   transaction        the current Transaction object, use to record pages for deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Coalesce(BPlusTreePage *sibling, BPlusTreePage *node, InternalPage *parent, int index,
                              Transaction *transaction) {}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair to the front of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   sibling            sibling page of input "node"
 * @param   node               input from method CoalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @param   index              index of pointer to "node" within "parent"
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(BPlusTreePage *sibling, BPlusTreePage *node, InternalPage *parent, int index) {}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() { 
  KeyType temp_key; //not used
  auto leaf_page = FindLeafPage(temp_key, true);
  auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  auto page_id = leaf_node->GetPageId();
  return INDEXITERATOR_TYPE(page_id, 0, buffer_pool_manager_);

 }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { 
  auto leaf_page = FindLeafPage(key, false);
  auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  int index = leaf_node->KeyIndex(key, comparator_);
  auto page_id = leaf_node->GetPageId();
  return INDEXITERATOR_TYPE(page_id, index, buffer_pool_manager_); 
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { 
  return INDEXITERATOR_TYPE(INT_MAX, INT_MAX, buffer_pool_manager_);
  // INDEXITERATOR_TYPE end_iterator = begin();
  // while (!end_iterator.isEnd()) {
  //   ++end_iterator;
  // }
  // return ++end_iterator;

}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) { 
  if(IsEmpty()) {
    return nullptr;  
  }
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage* page_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while(!page_node->IsLeafPage()){
    auto internal = reinterpret_cast<InternalPage *>(page_node);
    page_id_t child_page_id;
    if(leftMost){
      child_page_id = internal->ValueAt(0);
    }else{
      child_page_id = internal->Lookup(key, comparator_);
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(child_page_id); //return leaf found
    page_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(bool insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  LOG_INFO("# Check from the beginning of InsertFromFile");
  int64_t key;
  std::ifstream input(file_name);
  LOG_INFO("# Check from before the while loop of InsertFromFile");
  while (input) {
    LOG_INFO("# Check from InsertFromFile");
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * Tests depend on this function, DO NOT MODIFY.
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  std::stringstream result;
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    result << "Leaf Page: " << leaf->GetPageId() << " size: " << leaf->GetSize()
           << " parent: " << leaf->GetParentPageId() << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      result << leaf->KeyAt(i) << ",";
    }
    result << std::endl;
    result << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    result << "Internal Page: " << internal->GetPageId() << " size: " << internal->GetSize()
           << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      result << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    result << std::endl;
    result << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      result << ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
  return result.str();
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
