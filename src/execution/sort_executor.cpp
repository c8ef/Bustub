#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  sort_tuples_.clear();

  Tuple child_tuple{};
  RID child_rid{};

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    sort_tuples_.emplace_back(child_tuple, child_rid);
  }

  std::sort(sort_tuples_.begin(), sort_tuples_.end(), [this](std::pair<Tuple, RID> &lhs, std::pair<Tuple, RID> &rhs) {
    auto order_bys = plan_->GetOrderBy();
    for (auto &order_by : order_bys) {
      Value v_lhs = order_by.second->Evaluate(&lhs.first, child_executor_->GetOutputSchema());
      Value v_rhs = order_by.second->Evaluate(&rhs.first, child_executor_->GetOutputSchema());

      if (v_lhs.CompareEquals(v_rhs) != CmpBool::CmpTrue) {
        if (order_by.first == OrderByType::DEFAULT || order_by.first == OrderByType::ASC) {
          return v_lhs.CompareLessThan(v_rhs) == CmpBool::CmpTrue;
        }
        return v_lhs.CompareGreaterThan(v_rhs) == CmpBool::CmpTrue;
      }
    }
    return false;
  });

  sort_tuples_iter_ = sort_tuples_.cbegin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sort_tuples_iter_ == sort_tuples_.cend()) {
    return false;
  }

  *tuple = sort_tuples_iter_->first;
  *rid = sort_tuples_iter_->second;
  sort_tuples_iter_++;
  return true;
}

}  // namespace bustub
