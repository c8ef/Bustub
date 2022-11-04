//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), inserted_(false) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (inserted_) {
    return false;
  }
  inserted_ = true;

  Tuple child_tuple{};
  int insert_num = 0;

  std::string table_name = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_;
  std::vector<IndexInfo *> index_vector;
  for (const auto &index : exec_ctx_->GetCatalog()->GetTableIndexes(table_name)) {
    if (index->table_name_ == table_name) {
      index_vector.push_back(index);
    }
  }

  while (child_executor_->Next(&child_tuple, rid)) {
    insert_num += 1;
    exec_ctx_->GetCatalog()
        ->GetTable(plan_->TableOid())
        ->table_->InsertTuple(child_tuple, rid, exec_ctx_->GetTransaction());

    for (auto index : index_vector) {
      index->index_->InsertEntry(child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(),
                                                          *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
                                 *rid, exec_ctx_->GetTransaction());
    }
  }

  std::vector<Value> values{};
  values.push_back(Value(TypeId::INTEGER, insert_num));
  *tuple = Tuple{values, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
