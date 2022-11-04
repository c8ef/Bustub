//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (deleted_) {
    return false;
  }
  deleted_ = true;

  Tuple child_tuple{};
  int delete_num = 0;

  std::string table_name = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_;
  std::vector<IndexInfo *> index_vector;
  for (const auto &index : exec_ctx_->GetCatalog()->GetTableIndexes(table_name)) {
    if (index->table_name_ == table_name) {
      index_vector.push_back(index);
    }
  }

  while (child_executor_->Next(&child_tuple, rid)) {
    delete_num += 1;
    exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());

    for (auto index : index_vector) {
      index->index_->DeleteEntry(child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(),
                                                          *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
                                 *rid, exec_ctx_->GetTransaction());
    }
  }

  std::vector<Value> values{};
  values.emplace_back(Value(TypeId::INTEGER, delete_num));
  *tuple = Tuple{values, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
