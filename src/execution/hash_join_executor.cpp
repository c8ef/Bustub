//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::INNER || plan->GetJoinType() == JoinType::LEFT)) {
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  hash_join_table_.clear();
  output_.clear();

  Tuple right_tuple{};
  RID rid;
  while (right_child_->Next(&right_tuple, &rid)) {
    auto join_key = plan_->RightJoinKeyExpression().Evaluate(&right_tuple, plan_->GetRightPlan()->OutputSchema());
    hash_join_table_[HashUtil::HashValue(&join_key)].push_back(right_tuple);
  }

  if (plan_->GetJoinType() == JoinType::INNER) {
    Tuple left_tuple{};
    while (true) {
      if (!left_child_->Next(&left_tuple, &rid)) {
        break;
      }

      auto join_key = plan_->LeftJoinKeyExpression().Evaluate(&left_tuple, plan_->GetLeftPlan()->OutputSchema());
      if (hash_join_table_.count(HashUtil::HashValue(&join_key)) == 0) {
        continue;
      }

      auto possible_tuples = hash_join_table_[HashUtil::HashValue(&join_key)];
      for (const auto &tuple : possible_tuples) {
        auto right_join_key = plan_->RightJoinKeyExpression().Evaluate(&tuple, plan_->GetRightPlan()->OutputSchema());
        if (right_join_key.CompareEquals(join_key) == CmpBool::CmpTrue) {
          std::vector<Value> values{};
          values.reserve(plan_->GetLeftPlan()->OutputSchema().GetColumnCount() +
                         plan_->GetRightPlan()->OutputSchema().GetColumnCount());
          for (uint32_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); ++i) {
            values.push_back(left_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
          }
          for (uint32_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); ++i) {
            values.push_back(tuple.GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
          }
          output_.emplace_back(values, &GetOutputSchema());
        }
      }
    }
  }

  if (plan_->GetJoinType() == JoinType::LEFT) {
    Tuple left_tuple{};
    while (left_child_->Next(&left_tuple, &rid)) {
      auto join_key = plan_->LeftJoinKeyExpression().Evaluate(&left_tuple, plan_->GetLeftPlan()->OutputSchema());
      if (hash_join_table_.count(HashUtil::HashValue(&join_key)) == 0) {
        std::vector<Value> values{};
        values.reserve(plan_->GetLeftPlan()->OutputSchema().GetColumnCount() +
                       plan_->GetRightPlan()->OutputSchema().GetColumnCount());
        for (uint32_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); ++i) {
          values.push_back(left_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
        }
        auto columns = right_child_->GetOutputSchema().GetColumns();
        for (uint32_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); ++i) {
          values.push_back(ValueFactory::GetNullValueByType(columns[i].GetType()));
        }
        output_.emplace_back(values, &GetOutputSchema());

        continue;
      }

      auto possible_tuples = hash_join_table_[HashUtil::HashValue(&join_key)];
      bool find_a_match = false;
      for (const auto &tuple : possible_tuples) {
        auto right_join_key = plan_->RightJoinKeyExpression().Evaluate(&tuple, plan_->GetRightPlan()->OutputSchema());
        if (right_join_key.CompareEquals(join_key) == CmpBool::CmpTrue) {
          find_a_match = true;

          std::vector<Value> values{};
          values.reserve(plan_->GetLeftPlan()->OutputSchema().GetColumnCount() +
                         plan_->GetRightPlan()->OutputSchema().GetColumnCount());
          for (uint32_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); ++i) {
            values.push_back(left_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
          }
          for (uint32_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); ++i) {
            values.push_back(tuple.GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
          }
          output_.emplace_back(values, &GetOutputSchema());
        }
      }

      if (!find_a_match) {
        std::vector<Value> values{};
        values.reserve(plan_->GetLeftPlan()->OutputSchema().GetColumnCount() +
                       plan_->GetRightPlan()->OutputSchema().GetColumnCount());
        for (uint32_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); ++i) {
          values.push_back(left_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
        }
        auto columns = right_child_->GetOutputSchema().GetColumns();
        for (uint32_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); ++i) {
          values.push_back(ValueFactory::GetNullValueByType(columns[i].GetType()));
        }
        output_.emplace_back(values, &GetOutputSchema());
      }
    }
  }

  output_iter_ = output_.cbegin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (output_iter_ == output_.cend()) {
    return false;
  }
  *tuple = *output_iter_;
  output_iter_++;
  return true;
}

}  // namespace bustub
