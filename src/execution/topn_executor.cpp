#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  topn_tuples_.clear();

  Tuple child_tuple{};
  RID child_rid{};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    topn_tuples_.emplace_back(child_tuple, child_rid);
  }

  std::make_heap(std::begin(topn_tuples_), std::end(topn_tuples_),
                 [this](std::pair<Tuple, RID> &lhs, std::pair<Tuple, RID> &rhs) {
                   auto order_bys = plan_->GetOrderBy();
                   for (auto &order_by : order_bys) {
                     Value v_lhs = order_by.second->Evaluate(&lhs.first, child_executor_->GetOutputSchema());
                     Value v_rhs = order_by.second->Evaluate(&rhs.first, child_executor_->GetOutputSchema());

                     if (v_lhs.CompareEquals(v_rhs) != CmpBool::CmpTrue) {
                       if (order_by.first == OrderByType::DEFAULT || order_by.first == OrderByType::ASC) {
                         return v_lhs.CompareGreaterThan(v_rhs) == CmpBool::CmpTrue;
                       }
                       return v_lhs.CompareLessThan(v_rhs) == CmpBool::CmpTrue;
                     }
                   }
                   return false;
                 });
  n_ = plan_->GetN();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (n_ == 0 || topn_tuples_.empty()) {
    return false;
  }
  std::pop_heap(std::begin(topn_tuples_), std::end(topn_tuples_),
                [this](std::pair<Tuple, RID> &lhs, std::pair<Tuple, RID> &rhs) {
                  auto order_bys = plan_->GetOrderBy();
                  for (auto &order_by : order_bys) {
                    Value v_lhs = order_by.second->Evaluate(&lhs.first, child_executor_->GetOutputSchema());
                    Value v_rhs = order_by.second->Evaluate(&rhs.first, child_executor_->GetOutputSchema());

                    if (v_lhs.CompareEquals(v_rhs) != CmpBool::CmpTrue) {
                      if (order_by.first == OrderByType::DEFAULT || order_by.first == OrderByType::ASC) {
                        return v_lhs.CompareGreaterThan(v_rhs) == CmpBool::CmpTrue;
                      }
                      return v_lhs.CompareLessThan(v_rhs) == CmpBool::CmpTrue;
                    }
                  }
                  return false;
                });
  *tuple = topn_tuples_.back().first;
  *rid = topn_tuples_.back().second;
  topn_tuples_.pop_back();
  n_--;
  return true;
}

}  // namespace bustub
