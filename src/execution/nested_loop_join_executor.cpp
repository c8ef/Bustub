//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  RID rid;
  left_executor_->Next(&left_tuple_, &rid);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (plan_->GetJoinType() == JoinType::INNER) {
    Tuple right_tuple{};

    while (true) {
      if (!right_executor_->Next(&right_tuple, rid)) {
        right_executor_->Init();

        if (!left_executor_->Next(&left_tuple_, rid)) {
          return false;
        }
        right_executor_->Next(&right_tuple, rid);
      }
      auto value = plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                   right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        std::vector<Value> values{};
        values.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                       right_executor_->GetOutputSchema().GetColumnCount());
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        return true;
      }
    }
  }
  if (plan_->GetJoinType() == JoinType::LEFT) {
    Tuple right_tuple{};

    if (final_) {
      return false;
    }

    while (true) {
      if (!right_executor_->Next(&right_tuple, rid)) {
        right_executor_->Init();
        if (left_match_) {
          left_match_ = false;
          if (!left_executor_->Next(&left_tuple_, rid)) {
            return false;
          }
          right_executor_->Next(&right_tuple, rid);
        } else {
          std::vector<Value> values{};
          values.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                         right_executor_->GetOutputSchema().GetColumnCount());
          for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
            values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
          }

          auto columns = right_executor_->GetOutputSchema().GetColumns();
          for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
            values.push_back(ValueFactory::GetNullValueByType(columns[i].GetType()));
          }
          *tuple = Tuple{values, &GetOutputSchema()};

          if (!left_executor_->Next(&left_tuple_, rid)) {
            final_ = true;
          }
          return true;
        }
      }
      auto value = plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                   right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        std::vector<Value> values{};
        values.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                       right_executor_->GetOutputSchema().GetColumnCount());
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        left_match_ = true;
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
