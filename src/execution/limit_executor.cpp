//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  // reset the state machine as always!
  limit_tuples_.clear();

  Tuple child_tuple{};
  RID child_rid{};
  for (size_t i = 0; i < plan_->GetLimit(); ++i) {
    if (!child_executor_->Next(&child_tuple, &child_rid)) {
      break;
    }
    limit_tuples_.emplace_back(child_tuple, child_rid);
  }
  limit_tuples_iter_ = limit_tuples_.cbegin();
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (limit_tuples_iter_ == limit_tuples_.cend()) {
    return false;
  }
  *tuple = limit_tuples_iter_->first;
  *rid = limit_tuples_iter_->second;
  limit_tuples_iter_++;
  return true;
}

}  // namespace bustub
