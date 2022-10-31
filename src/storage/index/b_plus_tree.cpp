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
  if (root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto tree_page = reinterpret_cast<BPlusTreePage *>(page);
  auto last_page_id = root_page_id_;
  while (!tree_page->IsLeafPage()) {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    page_id_t child = INVALID_PAGE_ID;
    for (int i = 1; i < inner->GetSize(); ++i) {
      if (comparator_(inner->KeyAt(i), key) > 0) {
        child = inner->ValueAt(i - 1);
        break;
      }
    }
    if (child == INVALID_PAGE_ID) {
      child = inner->ValueAt(tree_page->GetSize() - 1);
    }
    buffer_pool_manager_->UnpinPage(last_page_id, false);
    last_page_id = child;
    page = buffer_pool_manager_->FetchPage(last_page_id);
    tree_page = reinterpret_cast<BPlusTreePage *>(page);
  }
  auto *leaf = reinterpret_cast<LeafPage *>(page);
  for (int i = 0; i < leaf->GetSize(); ++i) {
    if (comparator_(key, leaf->KeyAt(i)) == 0) {
      result->push_back(leaf->ValueAt(i));
      buffer_pool_manager_->UnpinPage(last_page_id, false);
      return true;
    }
  }
  buffer_pool_manager_->UnpinPage(last_page_id, false);
  return false;
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
  // the tree is empty
  // create an empty leaf node, which is also the root
  if (root_page_id_ == INVALID_PAGE_ID) {
    auto new_page = buffer_pool_manager_->NewPage(&root_page_id_);
    auto page_data = reinterpret_cast<LeafPage *>(new_page);
    UpdateRootPageId();
    page_data->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    page_data->SetNextPageId(INVALID_PAGE_ID);
    return InsertInLeaf(new_page, key, value, transaction);
  }

  auto root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto tree_page = reinterpret_cast<BPlusTreePage *>(root_page);
  // root page is leaf page, not full
  if (tree_page->IsLeafPage() && tree_page->GetSize() < tree_page->GetMaxSize()) {
    return InsertInLeaf(root_page, key, value, transaction);
  }
  if (tree_page->IsLeafPage() && tree_page->GetSize() == tree_page->GetMaxSize()) {
    return InsertWithSplit(root_page, key, value);
  }

  auto last_page_id = root_page_id_;
  auto inner_page = reinterpret_cast<BPlusTreePage *>(root_page);
  while (!inner_page->IsLeafPage()) {
    auto *inner = reinterpret_cast<InternalPage *>(root_page);
    page_id_t child = INVALID_PAGE_ID;
    for (int i = 1; i < inner->GetSize(); ++i) {
      if (comparator_(inner->KeyAt(i), key) > 0) {
        child = inner->ValueAt(i - 1);
        break;
      }
    }
    if (child == INVALID_PAGE_ID) {
      child = inner->ValueAt(inner->GetSize() - 1);
    }
    buffer_pool_manager_->UnpinPage(last_page_id, false);
    last_page_id = child;
    root_page = buffer_pool_manager_->FetchPage(last_page_id);
    inner_page = reinterpret_cast<BPlusTreePage *>(root_page);
  }
  auto *leaf = reinterpret_cast<LeafPage *>(root_page);
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    return InsertInLeaf(root_page, key, value, transaction);
  }
  return InsertWithSplit(root_page, key, value, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertWithSplit(Page *page, const KeyType &key, const ValueType &value, Transaction *transaction)
    -> bool {
  auto *leaf = reinterpret_cast<LeafPage *>(page);
  auto leaf_page_id = page->GetPageId();
  page_id_t split_page_id;
  auto split_page = buffer_pool_manager_->NewPage(&split_page_id);
  auto split_tree_page = reinterpret_cast<LeafPage *>(split_page);
  auto inner_data = reinterpret_cast<MappingType *>(page->GetData() + LEAF_PAGE_HEADER_SIZE);
  auto split_inner_data = reinterpret_cast<MappingType *>(split_page->GetData() + LEAF_PAGE_HEADER_SIZE);

  // init the metadata
  split_tree_page->Init(split_page_id, INVALID_PAGE_ID, leaf_max_size_);
  split_tree_page->SetNextPageId(leaf->GetNextPageId());

  // store the element in the buffer
  auto *temp_buffer = new MappingType[leaf->GetSize() + 1];

  for (int i = 0; i < leaf->GetSize(); ++i) {
    if (comparator_(inner_data[i].first, key) == 0) {
      buffer_pool_manager_->UnpinPage(leaf_page_id, false);
      buffer_pool_manager_->DeletePage(split_page_id);
      delete[] temp_buffer;
      return false;
    }
    temp_buffer[i] = inner_data[i];
  }
  temp_buffer[leaf->GetSize()].first = key;
  temp_buffer[leaf->GetSize()].second = value;
  std::sort(temp_buffer, temp_buffer + leaf->GetSize() + 1,
            [this](auto &&lhs, auto &&rhs) { return comparator_(lhs.first, rhs.first) < 0; });

  // re-distribute
  auto old_size = leaf->GetSize() + 1;
  leaf->SetSize(old_size / 2);
  split_tree_page->SetSize(old_size - old_size / 2);
  for (int i = 0; i < old_size; ++i) {
    if (i < old_size / 2) {
      inner_data[i] = temp_buffer[i];
    } else {
      split_inner_data[i - leaf->GetSize()] = temp_buffer[i];
    }
  }
  leaf->SetNextPageId(split_page_id);

  // setup parent
  if (leaf->GetParentPageId() == INVALID_PAGE_ID) {
    page_id_t parent_page_id;
    auto parent_page = buffer_pool_manager_->NewPage(&parent_page_id);
    auto parent_tree_page = reinterpret_cast<InternalPage *>(parent_page);
    auto temp_page_id = parent_page_id;
    parent_tree_page->Init(temp_page_id, INVALID_PAGE_ID, internal_max_size_);
    auto parent_inner_data =
        reinterpret_cast<std::pair<KeyType, page_id_t> *>(parent_page->GetData() + INTERNAL_PAGE_HEADER_SIZE);

    parent_inner_data[parent_tree_page->GetSize() + 1].first = split_inner_data[0].first;
    parent_inner_data[parent_tree_page->GetSize()].second = leaf_page_id;
    parent_inner_data[parent_tree_page->GetSize() + 1].second = split_page_id;
    parent_tree_page->IncreaseSize(2);

    // update parent
    leaf->SetParentPageId(parent_page_id);
    split_tree_page->SetParentPageId(parent_page_id);

    // update root
    auto old_root_id = root_page_id_;
    root_page_id_ = parent_page_id;
    UpdateRootPageId();

    // unpin all used page
    buffer_pool_manager_->UnpinPage(old_root_id, true);
    buffer_pool_manager_->UnpinPage(split_page_id, true);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    delete[] temp_buffer;
    return true;
  }
  auto parent_page = buffer_pool_manager_->FetchPage(leaf->GetParentPageId());
  auto parent_tree_page = reinterpret_cast<InternalPage *>(parent_page);
  if (parent_tree_page->GetSize() < parent_tree_page->GetMaxSize()) {
    if (!InsertInternal(parent_page, split_inner_data[0].first, split_page_id)) {
      delete[] temp_buffer;
      return false;
    }
    split_tree_page->SetParentPageId(leaf->GetParentPageId());
    buffer_pool_manager_->UnpinPage(split_page_id, true);
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
    delete[] temp_buffer;
    return true;
  }
  if (!InsertWithSplitInternal(parent_page, split_inner_data[0].first, split_page_id)) {
    delete[] temp_buffer;
    return false;
  }
  buffer_pool_manager_->UnpinPage(split_page_id, true);
  buffer_pool_manager_->UnpinPage(leaf_page_id, true);
  delete[] temp_buffer;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertWithSplitInternal(Page *page, const KeyType &key, const page_id_t &value,
                                             Transaction *transaction) -> bool {
  auto *internal = reinterpret_cast<InternalPage *>(page);
  auto internal_page_id = page->GetPageId();
  page_id_t split_page_id;
  auto split_page = buffer_pool_manager_->NewPage(&split_page_id);
  auto *split_tree_page = reinterpret_cast<InternalPage *>(split_page);
  auto inner_data = reinterpret_cast<std::pair<KeyType, page_id_t> *>(page->GetData() + INTERNAL_PAGE_HEADER_SIZE);
  auto split_inner_data =
      reinterpret_cast<std::pair<KeyType, page_id_t> *>(split_page->GetData() + INTERNAL_PAGE_HEADER_SIZE);

  split_tree_page->Init(split_page_id, INVALID_PAGE_ID, internal_max_size_);

  auto *temp_buffer = new std::pair<KeyType, page_id_t>[internal->GetSize() + 1];

  for (int i = 0; i < internal->GetSize(); ++i) {
    if (comparator_(inner_data[i].first, key) == 0) {
      buffer_pool_manager_->UnpinPage(internal_page_id, false);
      buffer_pool_manager_->DeletePage(split_page_id);
      delete[] temp_buffer;
      return false;
    }
    temp_buffer[i] = inner_data[i];
  }
  temp_buffer[internal->GetSize()].first = key;
  temp_buffer[internal->GetSize()].second = value;

  std::sort(temp_buffer, temp_buffer + internal->GetSize() + 1,
            [this](auto &&lhs, auto &&rhs) { return comparator_(lhs.first, rhs.first) < 0; });

  auto old_size = internal->GetSize() + 1;
  internal->SetSize(old_size / 2);
  split_tree_page->SetSize(old_size - old_size / 2);
  auto push_up_index = temp_buffer[old_size / 2].first;
  for (int i = 0; i < old_size; ++i) {
    if (i < old_size / 2) {
      inner_data[i] = temp_buffer[i];
    } else {
      split_inner_data[i - internal->GetSize()] = temp_buffer[i];
    }
  }

  for (int i = 0; i < split_tree_page->GetSize(); ++i) {
    auto change_page = buffer_pool_manager_->FetchPage(split_inner_data[i].second);
    auto change_tree_page = reinterpret_cast<BPlusTreePage *>(change_page);
    change_tree_page->SetParentPageId(split_page_id);
    buffer_pool_manager_->UnpinPage(split_inner_data[i].second, true);
  }

  if (internal->GetParentPageId() == INVALID_PAGE_ID) {
    page_id_t parent_page_id;
    auto parent_page = buffer_pool_manager_->NewPage(&parent_page_id);
    auto parent_tree_page = reinterpret_cast<InternalPage *>(parent_page);
    auto temp_page_id = parent_page_id;
    parent_tree_page->Init(temp_page_id, INVALID_PAGE_ID, internal_max_size_);
    auto parent_inner_data =
        reinterpret_cast<std::pair<KeyType, page_id_t> *>(parent_page->GetData() + INTERNAL_PAGE_HEADER_SIZE);

    parent_inner_data[parent_tree_page->GetSize() + 1].first = push_up_index;
    parent_inner_data[parent_tree_page->GetSize()].second = internal_page_id;
    parent_inner_data[parent_tree_page->GetSize() + 1].second = split_page_id;
    parent_tree_page->IncreaseSize(2);

    internal->SetParentPageId(parent_page_id);
    split_tree_page->SetParentPageId(parent_page_id);

    root_page_id_ = parent_page_id;
    UpdateRootPageId();

    buffer_pool_manager_->UnpinPage(split_page_id, true);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    buffer_pool_manager_->UnpinPage(internal_page_id, true);
    delete[] temp_buffer;
    return true;
  }
  auto parent_page = buffer_pool_manager_->FetchPage(internal->GetParentPageId());
  auto parent_tree_page = reinterpret_cast<InternalPage *>(parent_page);
  if (parent_tree_page->GetSize() < parent_tree_page->GetMaxSize()) {
    if (!InsertInternal(parent_page, push_up_index, split_page_id)) {
      delete[] temp_buffer;
      return false;
    }
    split_tree_page->SetParentPageId(internal->GetParentPageId());
    buffer_pool_manager_->UnpinPage(split_page_id, true);
    buffer_pool_manager_->UnpinPage(internal_page_id, true);
    delete[] temp_buffer;
    return true;
  }
  if (!InsertWithSplitInternal(parent_page, push_up_index, split_page_id)) {
    delete[] temp_buffer;
    return false;
  }
  buffer_pool_manager_->UnpinPage(split_page_id, true);
  buffer_pool_manager_->UnpinPage(internal_page_id, true);
  delete[] temp_buffer;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInLeaf(Page *page, const KeyType &key, const ValueType &value, Transaction *transaction)
    -> bool {
  auto tree_page = reinterpret_cast<LeafPage *>(page);
  auto inner_data = reinterpret_cast<MappingType *>(page->GetData() + LEAF_PAGE_HEADER_SIZE);
  if (tree_page->GetSize() == 0) {
    inner_data[0].first = key;
    inner_data[0].second = value;
  } else {
    int insert_pos = 0;
    for (; insert_pos < tree_page->GetSize(); ++insert_pos) {
      if (comparator_(inner_data[insert_pos].first, key) == 0) {
        buffer_pool_manager_->UnpinPage(root_page_id_, false);
        return false;
      }
      if (comparator_(key, inner_data[insert_pos].first) < 0) {
        break;
      }
    }

    if (insert_pos == tree_page->GetSize()) {
      inner_data[insert_pos].first = key;
      inner_data[insert_pos].second = value;
    } else {
      for (int i = tree_page->GetSize() - 1; i >= insert_pos; --i) {
        inner_data[i + 1] = inner_data[i];
      }
      inner_data[insert_pos].first = key;
      inner_data[insert_pos].second = value;
    }
  }

  tree_page->IncreaseSize(1);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInternal(Page *page, const KeyType &key, const page_id_t &value, Transaction *transaction)
    -> bool {
  auto *tree_page = reinterpret_cast<InternalPage *>(page);
  auto inner_data = reinterpret_cast<std::pair<KeyType, page_id_t> *>(page->GetData() + INTERNAL_PAGE_HEADER_SIZE);

  // we assume this particular internal page always exists at least one element
  int insert_pos = 1;
  for (; insert_pos < tree_page->GetSize(); ++insert_pos) {
    if (comparator_(inner_data[insert_pos].first, key) == 0) {
      buffer_pool_manager_->UnpinPage(root_page_id_, false);
      return false;
    }
    if (comparator_(key, inner_data[insert_pos].first) < 0) {
      break;
    }
  }
  if (insert_pos == tree_page->GetSize()) {
    inner_data[insert_pos].first = key;
    inner_data[insert_pos].second = value;
  } else {
    for (int i = tree_page->GetSize() - 1; i >= insert_pos; --i) {
      inner_data[i + 1] = inner_data[i];
    }
    inner_data[insert_pos].first = key;
    inner_data[insert_pos].second = value;
  }

  tree_page->IncreaseSize(1);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return true;
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

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
  if (root_page_id_ == INVALID_PAGE_ID) {
    throw std::runtime_error("the given tree is empty");
  }
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto tree_page = reinterpret_cast<BPlusTreePage *>(page);
  auto last_page_id = root_page_id_;

  while (!tree_page->IsLeafPage()) {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    page_id_t child = inner->ValueAt(0);
    buffer_pool_manager_->UnpinPage(last_page_id, false);
    last_page_id = child;
    page = buffer_pool_manager_->FetchPage(last_page_id);
    tree_page = reinterpret_cast<BPlusTreePage *>(page);
  }

  return INDEXITERATOR_TYPE(false, buffer_pool_manager_, 0, comparator_, page);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    throw std::runtime_error("the given tree is empty");
  }
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto tree_page = reinterpret_cast<BPlusTreePage *>(page);
  auto last_page_id = root_page_id_;

  while (!tree_page->IsLeafPage()) {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    page_id_t child = INVALID_PAGE_ID;
    for (int i = 1; i < inner->GetSize(); ++i) {
      if (comparator_(inner->KeyAt(i), key) > 0) {
        child = inner->ValueAt(i - 1);
        break;
      }
    }
    if (child == INVALID_PAGE_ID) {
      child = inner->ValueAt(tree_page->GetSize() - 1);
    }
    buffer_pool_manager_->UnpinPage(last_page_id, false);
    last_page_id = child;
    page = buffer_pool_manager_->FetchPage(last_page_id);
    tree_page = reinterpret_cast<BPlusTreePage *>(page);
  }

  auto *leaf = reinterpret_cast<LeafPage *>(page);
  for (int i = 0; i < leaf->GetSize(); ++i) {
    if (comparator_(key, leaf->KeyAt(i)) == 0) {
      return INDEXITERATOR_TYPE(false, buffer_pool_manager_, i, comparator_, page);
    }
  }
  buffer_pool_manager_->UnpinPage(last_page_id, false);
  throw std::runtime_error("the key does not exist in the tree");
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(true, nullptr, 0, comparator_, nullptr); }

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

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
