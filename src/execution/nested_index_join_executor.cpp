//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      tree_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
          exec_ctx_->GetCatalog()->GetIndex(plan->GetIndexOid())->index_.get())) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  assert(child_executor_ != nullptr);
  child_executor_->Init();
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
      exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get());
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (plan_->GetJoinType() == JoinType::INNER) {
    Tuple child_tuple{};
    while (true) {
      if (!child_executor_->Next(&child_tuple, rid)) {
        return false;
      }
      auto search_key = plan_->KeyPredicate()->Evaluate(&child_tuple, child_executor_->GetOutputSchema());
      std::vector<Value> search_values;
      search_values.push_back(search_key);

      std::vector<RID> search_result;
      tree_->ScanKey(Tuple{search_values, &exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->key_schema_},
                     &search_result, exec_ctx_->GetTransaction());

      if (!search_result.empty()) {
        Tuple inner_tuple{};
        exec_ctx_->GetCatalog()
            ->GetTable(plan_->GetInnerTableOid())
            ->table_->GetTuple(search_result[0], &inner_tuple, exec_ctx_->GetTransaction());

        std::vector<Value> values{};
        for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())->schema_.GetColumnCount();
             ++i) {
          values.push_back(
              inner_tuple.GetValue(&exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())->schema_, i));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        return true;
      }
    }
  }
  if (plan_->GetJoinType() == JoinType::LEFT) {
    Tuple child_tuple{};
    if (!child_executor_->Next(&child_tuple, rid)) {
      return false;
    }

    auto search_key = plan_->KeyPredicate()->Evaluate(&child_tuple, child_executor_->GetOutputSchema());
    std::vector<Value> search_values;
    search_values.push_back(search_key);

    std::vector<RID> search_result;
    tree_->ScanKey(Tuple{search_values, &exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->key_schema_},
                   &search_result, exec_ctx_->GetTransaction());

    if (!search_result.empty()) {
      Tuple inner_tuple{};
      exec_ctx_->GetCatalog()
          ->GetTable(plan_->GetInnerTableOid())
          ->table_->GetTuple(search_result[0], &inner_tuple, exec_ctx_->GetTransaction());

      std::vector<Value> values{};
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())->schema_.GetColumnCount();
           ++i) {
        values.push_back(
            inner_tuple.GetValue(&exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())->schema_, i));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
    std::vector<Value> values{};
    for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values.push_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
    }

    auto columns = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())->schema_.GetColumns();
    for (uint32_t i = 0; i < exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())->schema_.GetColumnCount();
         ++i) {
      values.push_back(ValueFactory::GetNullValueByType(columns[i].GetType()));
    }
    *tuple = Tuple{values, &GetOutputSchema()};
    return true;
  }
  return false;
}

}  // namespace bustub
