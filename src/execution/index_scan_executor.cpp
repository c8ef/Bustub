//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tree_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
          exec_ctx_->GetCatalog()->GetIndex(plan->GetIndexOid())->index_.get())),
      tree_iter_(tree_->GetBeginIterator()) {}

void IndexScanExecutor::Init() {
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
      exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get());
  tree_iter_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tree_iter_ == tree_->GetEndIterator()) {
    return false;
  }

  *rid = (*tree_iter_).second;
  Tuple emit_tuple{};

  exec_ctx_->GetCatalog()
      ->GetTable(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->table_name_)
      ->table_->GetTuple(*rid, &emit_tuple, exec_ctx_->GetTransaction());
  *tuple = emit_tuple;

  ++tree_iter_;
  return true;
}

}  // namespace bustub
