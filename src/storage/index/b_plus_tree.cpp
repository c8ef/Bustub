#include <algorithm>
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
      internal_max_size_(internal_max_size + 1) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
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
  auto leaf_page = FindLeafPageByOperation(key, Operation::kSearch, transaction).first;
  auto *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  ValueType v;
  auto exist = node->LookUp(key, &v, comparator_);

  leaf_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

  if (!exist) {
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
  // printf("insert %ld {%d|%d}\n", key.ToString(), leaf_max_size_, internal_max_size_);
  {
    // only lock update root page id operation
    std::scoped_lock<std::mutex> l{root_page_id_latch_};

    if (IsEmpty()) {
      StartNewTree(key, value);
      return true;
    }
  }
  return InsertIntoLeaf(key, value, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (page == nullptr) {
    throw std::runtime_error{"cannot allocate new page in the buffer pool!"};
  }

  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  leaf->Insert(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  // if insert record != 0
  // then will insert a new record
  // otherwise update record
  UpdateRootPageId(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  auto [leaf_page, is_root_page_id_latched] = FindLeafPageByOperation(key, Operation::kInsert, txn);
  auto *node = reinterpret_cast<LeafPage *>(leaf_page);

  auto size = node->GetSize();
  auto new_size = node->Insert(key, value, comparator_);

  // already exist
  if (new_size == size) {
    if (is_root_page_id_latched) {
      root_page_id_latch_.unlock();
    }
    ClearTransactionPageSetAndUnpinEach(txn);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }

  // not full
  if (new_size < leaf_max_size_) {
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }

  // leaf is full
  auto sibling_leaf_node = Split(node);
  sibling_leaf_node->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(sibling_leaf_node->GetPageId());

  auto push_up_key = sibling_leaf_node->KeyAt(0);
  InsertIntoParent(node, push_up_key, sibling_leaf_node, txn);

  // after insert in parent, release the lock of leaf
  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_leaf_node->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *txn) {
  if (old_node->IsRootPage()) {
    auto page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (page == nullptr) {
      throw std::runtime_error{"cannot allocate new page in the buffer pool!"};
    }

    auto *new_root = reinterpret_cast<InternalPage *>(page->GetData());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    old_node->SetParentPageId(new_root->GetPageId());
    new_node->SetParentPageId(new_root->GetPageId());

    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    UpdateRootPageId(0);

    // if leaf page is going to split to new root
    // root latch has already held
    root_page_id_latch_.unlock();
    ClearTransactionPageSetAndUnpinEach(txn);
    return;
  }

  auto parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  auto new_size = parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  if (new_size < internal_max_size_) {
    // parent not split
    // free all latches
    ClearTransactionPageSetAndUnpinEach(txn);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }

  auto parent_new_sibling_node = Split(parent_node);
  KeyType new_key = parent_new_sibling_node->KeyAt(0);
  InsertIntoParent(parent_node, new_key, parent_new_sibling_node, txn);

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_new_sibling_node->GetPageId(), true);
}

// use another template argument to handle both LeafPage and InternalPage
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw std::runtime_error{"cannot allocate new page in the buffer pool!"};
  }

  N *new_node = reinterpret_cast<N *>(page->GetData());
  new_node->SetPageType(node->GetPageType());

  if (node->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(node);
    auto *new_leaf = reinterpret_cast<LeafPage *>(new_node);

    new_leaf->Init(page->GetPageId(), node->GetParentPageId(), leaf_max_size_);
    leaf->MoveHalfTo(new_leaf);
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto *new_internal = reinterpret_cast<InternalPage *>(new_node);

    new_internal->Init(page->GetPageId(), node->GetParentPageId(), internal_max_size_);
    internal->MoveHalfTo(new_internal, buffer_pool_manager_);
  }
  return new_node;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // printf("remove %ld {%d|%d}\n", key.ToString(), leaf_max_size_, internal_max_size_);
  if (IsEmpty()) {
    return;
  }

  auto [leaf_page, is_root_page_id_latched] = FindLeafPageByOperation(key, Operation::kDelete, transaction);
  auto *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  // given key doesn't exist
  if (node->GetSize() == node->RemoveAndDeleteRecord(key, comparator_)) {
    if (is_root_page_id_latched) {
      root_page_id_latch_.unlock();
    }
    ClearTransactionPageSetAndUnpinEach(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }

  auto node_should_delete = CoalesceOrRedistribute(node, transaction, is_root_page_id_latched);
  leaf_page->WUnlatch();

  if (node_should_delete) {
    transaction->AddIntoDeletedPageSet(node->GetPageId());
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  std::for_each(transaction->GetDeletedPageSet()->begin(), transaction->GetDeletedPageSet()->end(),
                [this](const page_id_t page_id) { buffer_pool_manager_->DeletePage(page_id); });
  transaction->GetDeletedPageSet()->clear();
}

// return true means need delete
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *txn, bool is_root_page_id_latched) -> bool {
  if (node->IsRootPage()) {
    auto root_should_delete = AdjustRoot(node, is_root_page_id_latched);
    ClearTransactionPageSetAndUnpinEach(txn);
    return root_should_delete;
  }

  if (node->GetSize() >= node->GetMinSize()) {
    ClearTransactionPageSetAndUnpinEach(txn);
    return false;
  }

  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  auto index = parent_node->ValueIndex(node->GetPageId());

  auto sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(index == 0 ? 1 : index - 1));
  sibling_page->WLatch();

  N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());

  if (node->GetSize() + sibling_node->GetSize() >= node->GetMaxSize()) {
    Redistribute(sibling_node, node, parent_node, index, is_root_page_id_latched);
    ClearTransactionPageSetAndUnpinEach(txn);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return false;
  }

  auto parent_node_should_delete = Coalesce(&sibling_node, &node, &parent_node, index, txn, is_root_page_id_latched);

  if (parent_node_should_delete) {
    txn->AddIntoDeletedPageSet(parent_node->GetPageId());
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  sibling_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *txn, bool is_root_page_id_latched) -> bool {
  auto key_index = index;
  if (index == 0) {
    key_index = 1;
    // sibling | node
    std::swap(node, neighbor_node);
  }

  auto middle_key = (*parent)->KeyAt(key_index);

  if ((*node)->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(*node);
    auto *prev_leaf_node = reinterpret_cast<LeafPage *>(*neighbor_node);
    leaf_node->MoveAllTo(prev_leaf_node);
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(*node);
    auto *internal_leaf_node = reinterpret_cast<InternalPage *>(*neighbor_node);
    internal_node->MoveAllTo(internal_leaf_node, middle_key, buffer_pool_manager_);
  }

  (*parent)->Remove(key_index);

  return CoalesceOrRedistribute(*parent, txn, is_root_page_id_latched);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node,
                                  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                                  bool is_root_page_id_latched) {
  if (is_root_page_id_latched) {
    root_page_id_latch_.unlock();
  }

  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *neighbor_leaf_node = reinterpret_cast<LeafPage *>(neighbor_node);

    if (index == 0) {
      // node | sibling
      neighbor_leaf_node->MoveFirstToEndOf(leaf_node);
      parent->SetKeyAt(1, neighbor_leaf_node->KeyAt(0));
    } else {
      // sibling | node
      neighbor_leaf_node->MoveLastToFrontOf(leaf_node);
      parent->SetKeyAt(index, leaf_node->KeyAt(0));
    }
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *neighbor_internal_node = reinterpret_cast<InternalPage *>(neighbor_node);

    if (index == 0) {
      neighbor_internal_node->MoveFirstToEndOf(internal_node, parent->KeyAt(1), buffer_pool_manager_);
      parent->SetKeyAt(1, neighbor_internal_node->KeyAt(0));
    } else {
      neighbor_internal_node->MoveLastToFrontOf(internal_node, parent->KeyAt(index), buffer_pool_manager_);
      parent->SetKeyAt(index, internal_node->KeyAt(0));
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *node, bool is_root_page_id_latched) -> bool {
  // 1. delete the last element in the root page, but the root page still has a child
  // 2. delete the last child in the whole tree
  if (!node->IsLeafPage() && node->GetSize() == 1) {
    auto *root_node = reinterpret_cast<InternalPage *>(node);
    auto only_child_page = buffer_pool_manager_->FetchPage(root_node->ValueAt(0));
    auto *only_child_node = reinterpret_cast<BPlusTreePage *>(only_child_page->GetData());
    only_child_node->SetParentPageId(INVALID_PAGE_ID);

    root_page_id_ = only_child_node->GetPageId();
    UpdateRootPageId(0);

    if (is_root_page_id_latched) {
      root_page_id_latch_.unlock();
    }
    buffer_pool_manager_->UnpinPage(only_child_page->GetPageId(), true);
    return true;
  }

  if (is_root_page_id_latched) {
    root_page_id_latch_.unlock();
  }
  return node->IsLeafPage() && node->GetSize() == 0;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto left_most_page = FindLeafPageByOperation(KeyType(), Operation::kSearch, nullptr, true, false).first;
  return INDEXITERATOR_TYPE(buffer_pool_manager_, left_most_page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto leaf_page = FindLeafPageByOperation(key, Operation::kSearch).first;
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  auto idx = leaf_node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, idx);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  auto right_most_page = FindLeafPageByOperation(KeyType(), Operation::kSearch, nullptr, false, true).first;
  auto leaf_node = reinterpret_cast<LeafPage *>(right_most_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, right_most_page, leaf_node->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
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
void BPLUSTREE_TYPE::ClearTransactionPageSetAndUnpinEach(Transaction *txn) const {
  std::for_each(txn->GetPageSet()->begin(), txn->GetPageSet()->end(), [this](Page *page) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  });
  txn->GetPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ClearTransactionPageSet(Transaction *txn) const {
  std::for_each(txn->GetPageSet()->begin(), txn->GetPageSet()->end(), [](Page *page) { page->WUnlatch(); });
  txn->GetPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageByOperation(const KeyType &key, Operation operation, Transaction *transaction,
                                             bool left_most, bool right_most) -> std::pair<Page *, bool> {
  root_page_id_latch_.lock();
  auto is_root_page_id_latched = true;
  assert(root_page_id_ != INVALID_PAGE_ID);

  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *node = reinterpret_cast<BPlusTreePage *>(page);
  if (operation == Operation::kSearch) {
    page->RLatch();
    is_root_page_id_latched = false;
    root_page_id_latch_.unlock();
  } else {
    page->WLatch();
    // if root is safe, unlock root latch
    // notice that root page can less than half full
    if ((operation == Operation::kInsert && node->GetSize() < node->GetMaxSize() - 1) ||
        (operation == Operation::kDelete && node->GetSize() > 2)) {
      is_root_page_id_latched = false;
      root_page_id_latch_.unlock();
    }
  }

  while (!node->IsLeafPage()) {
    auto *inner = reinterpret_cast<InternalPage *>(node);

    page_id_t child_page_id;
    if (left_most) {
      child_page_id = inner->ValueAt(0);
    } else if (right_most) {
      child_page_id = inner->ValueAt(inner->GetSize() - 1);
    } else {
      child_page_id = inner->LookUp(key, comparator_);
    }
    assert(child_page_id > 0);

    auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
    auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    if (operation == Operation::kSearch) {
      // if is reading, just acquire the child latch and release the parent latch
      child_page->RLatch();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else if (operation == Operation::kInsert) {
      child_page->WLatch();
      transaction->AddIntoPageSet(page);

      if (child_node->GetSize() < child_node->GetMaxSize() - 1) {
        if (is_root_page_id_latched) {
          is_root_page_id_latched = false;
          root_page_id_latch_.unlock();
        }
        ClearTransactionPageSetAndUnpinEach(transaction);
      }
    } else if (operation == Operation::kDelete) {
      child_page->WLatch();
      transaction->AddIntoPageSet(page);

      if (child_node->GetSize() > child_node->GetMinSize()) {
        if (is_root_page_id_latched) {
          is_root_page_id_latched = false;
          root_page_id_latch_.unlock();
        }
        ClearTransactionPageSetAndUnpinEach(transaction);
      }
    }

    page = child_page;
    node = child_node;
  }
  return std::make_pair(page, is_root_page_id_latched);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
