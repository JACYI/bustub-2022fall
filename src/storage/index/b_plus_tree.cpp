#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

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
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if(transaction != nullptr) {
    root_latch_.RLock();
  }
  auto leaf_page = FindLeaf(key, SEARCH, transaction);
  auto *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  ValueType v;
  bool existed = node->Lookup(key, &v, comparator_);
  if(transaction != nullptr) {
    root_latch_.RUnlock();
  }

  leaf_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

  if(!existed) {
    return false;
  }
  result->push_back(v);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_latch_.WLock();
  transaction->AddIntoPageSet(nullptr); // nullptr represents root latch
  if(IsEmpty()) {
    StartNewTree(key, value);
    ReleaseLatchFromQueue(transaction);
    return true;
  }

  return InsertIntoLeaf(key, value, transaction);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if(root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr, 0);
  }
  root_latch_.RLock();
  auto leaftmost_leaf = FindLeaf(KeyType(), Operation::SEARCH, nullptr, true);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaftmost_leaf, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if(root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr, 0);
  }
  root_latch_.RLock();
  auto target_page = FindLeaf(key, Operation::SEARCH, nullptr);
  auto leaf_node = reinterpret_cast<LeafPage *>(target_page->GetData());

  int index = leaf_node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, target_page, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if(root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(buffer_pool_manager_, nullptr, 0);
  }
  root_latch_.RLock();
  auto rightmost_page = FindLeaf(KeyType(), Operation::SEARCH,
                                 nullptr, false, true);
  auto leaf_page = reinterpret_cast<LeafPage *>(rightmost_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, rightmost_page, leaf_page->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  return root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
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
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
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
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
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
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
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
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
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
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, Operation operation, Transaction *transaction, bool leftMost, bool rightMost)
    -> Page * {
  assert(operation == Operation::SEARCH ? !(leftMost && rightMost) : transaction != nullptr);

  assert(root_page_id_ != INVALID_PAGE_ID);
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  // Add lock
  if (operation == Operation::SEARCH) {
    // search operation can lock case by case
    if(transaction != nullptr) {
      root_latch_.RUnlock();
    }
    page->RLatch();
  } else {
    page->WLatch();
    if (operation == Operation::DELETE && node->GetSize() > 2) {
      // after deleting size larger than or equals to 2, won't merge
      ReleaseLatchFromQueue(transaction);
    }
    if (operation == Operation::INSERT && node->IsLeafPage() && node->GetSize() < node->GetMaxSize() - 1) {
      // leaf node split when size equals to max size
      ReleaseLatchFromQueue(transaction);
    }
    if (operation == Operation::INSERT && !node->IsLeafPage() && node->GetSize() < node->GetMaxSize()) {
      // internal node split when size larger than max size
      ReleaseLatchFromQueue(transaction);
    }
  }

  while (!node->IsLeafPage()) {
    auto *i_node = reinterpret_cast<InternalPage *>(node);

    page_id_t child_node_page_id;
    if (leftMost) {
      child_node_page_id = i_node->ValueAt(0);
    } else if (rightMost) {
      child_node_page_id = i_node->ValueAt(i_node->GetSize() - 1);
    } else {
      child_node_page_id = i_node->Lookup(key, comparator_);
    }
    assert(child_node_page_id > 0);

    auto child_page = buffer_pool_manager_->FetchPage(child_node_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    //
    /**
     * synchronize the parent id of child node
     * The parent id of every node is not updated until you look up
     */
    child_node->SetParentPageId(i_node->GetPageId());

    if (operation == Operation::SEARCH) {
      child_page->RLatch();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else if (operation == Operation::INSERT) {
      child_page->WLatch();
      transaction->AddIntoPageSet(page);

      // child node is safe, release all locks on ancestors
      if (child_node->IsLeafPage() && child_node->GetSize() < child_node->GetMaxSize() - 1) {
        ReleaseLatchFromQueue(transaction);
      }
      if (!child_node->IsLeafPage() && child_node->GetSize() < child_node->GetMaxSize()) {
        ReleaseLatchFromQueue(transaction);
      }
    } else if (operation == Operation::DELETE) {
      child_page->WLatch();
      transaction->AddIntoPageSet(page);

      // child node is safe, release all locks on ancestors
      if (child_node->GetSize() > child_node->GetMinSize()) {
        ReleaseLatchFromQueue(transaction);
      }
    }

    page = child_page;
    node = child_node;
  }

  return page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatchFromQueue(Transaction *transaction) {
  if(transaction == nullptr) {
    return;
  }
  auto latch_stack_ = transaction->GetPageSet();
  while(!latch_stack_->empty()) {
    auto page = latch_stack_->back();
    if(page == nullptr) {
      root_latch_.WUnlock();
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
    latch_stack_->pop_back();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto page = buffer_pool_manager_->NewPage(&root_page_id_);

  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }

  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);

  leaf->Insert(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // 1. Find leaf page and insert into it
  auto leaf_page = FindLeaf(key, Operation::INSERT, transaction);
  auto *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  auto old_size = node->GetSize();
  auto new_size = node->Insert(key, value, comparator_);

  if(new_size == old_size){
    // Duplicate key, do not modify previous and return false
    ReleaseLatchFromQueue(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;

  }

  if(new_size < leaf_max_size_) {
    // Release latch and return
    ReleaseLatchFromQueue(transaction);
    leaf_page->WUnlatch();
    // insert successful, set for dirty page
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;

  }

  // New old_size = max old_size (leaf), split node
  auto new_node = SplitLeaf(node);

  // Add new node point (value) to parent node
  InsertIntoParent(node, new_node->KeyAt(0), new_node, transaction);

  // Release latch
  ReleaseLatchFromQueue(transaction);
  leaf_page->WUnlatch();

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage *node) -> LeafPage* {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  auto *new_node = reinterpret_cast<LeafPage *>(new_page->GetData());

  // Iinitialize the new node
  new_node->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);

  // Move elements from node to new node
  node->SplitAndSnd(new_node);

  // Fix the next page id for two node
  new_node->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_node->GetPageId());

  // Set dirty pages
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

  return new_node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitIntenal(InternalPage *node) -> InternalPage * {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  auto *new_node = reinterpret_cast<InternalPage *>(new_page->GetData());

  // Initialize
  new_node->Init(new_page_id, node->GetParentPageId(), internal_max_size_);

  node->SplitAndSnd(new_node);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

  return new_node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MoveHalf(InternalPage *from, InternalPage *to) {

}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *node, const KeyType &new_key, BPlusTreePage *new_node, Transaction *transaction) {
  // If is root page
  if(node->IsRootPage()){
    // Create new page for storage two nodes
    page_id_t new_page_id;
    auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
    if(new_page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "node is null");
    }

    // make new page for new root
    auto new_root_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root_page->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    // Update meta data page information
    root_page_id_ = new_page_id;
    UpdateHeaderInfo();

    // Insert node key-value into new root
    // N, K', N' (N node, N' new_node, K' new_node first key)

    new_root_page->SetValueAt(0, node->GetPageId());
    new_root_page->SetKeyAt(1, new_key);
    new_root_page->SetValueAt(1, new_node->GetPageId());
    new_root_page->SetSize(2);

    node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);

    buffer_pool_manager_->UnpinPage(new_page_id, true);
    ReleaseLatchFromQueue(transaction);

    return ;

  }

  auto parent_node = reinterpret_cast<InternalPage *>(
      buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());


  // Not the root page, Insert into parent page
  // Old value is old node's page id, new value is new node's page id
  parent_node->InsertAfterNode(node->GetPageId(), new_key, new_node->GetPageId());

  if(parent_node->GetSize() <= internal_max_size_) {
    // parent node is not full, insert and return
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    ReleaseLatchFromQueue(transaction);
    return ;

  }
  // parent node is full, need to split parent
  auto new_parent_node = SplitIntenal(parent_node);
  // the key at index 0 is usually non,
  // but after spliting the internal node, the key in index 0 of new internal contains the lowest key
  InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node, transaction);

  ReleaseLatchFromQueue(transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateHeaderInfo() {
  auto meta_page = buffer_pool_manager_->FetchPage(HEADER_PAGE_ID);
  auto header_page = reinterpret_cast<HeaderPage *>(meta_page->GetData());

  if(!header_page->UpdateRecord(index_name_, root_page_id_)){
    header_page->InsertRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(meta_page->GetPageId(), true);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
