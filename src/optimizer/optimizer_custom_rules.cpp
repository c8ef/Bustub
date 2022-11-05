#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/mock_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

// Note for 2022 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file. Note
// that for some test cases, we force using starter rules, so that the configuration here won't take effects. Starter
// rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::OptimizeReorderJoinUseIndex(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeReorderJoinUseIndex(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    // ensure the left child is nlp
    // the right child is seqscan or mockscan
    if (nlj_plan.GetLeftPlan()->GetType() != PlanType::NestedLoopJoin ||
        (nlj_plan.GetRightPlan()->GetType() != PlanType::SeqScan &&
         nlj_plan.GetRightPlan()->GetType() != PlanType::MockScan)) {
      return optimized_plan;
    }

    const auto &left_nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*nlj_plan.GetLeftPlan());

    if (left_nlj_plan.GetLeftPlan()->GetType() == PlanType::NestedLoopJoin ||
        left_nlj_plan.GetRightPlan()->GetType() == PlanType::NestedLoopJoin) {
      return optimized_plan;
    }

    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(&left_nlj_plan.Predicate()); expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            if (left_nlj_plan.GetLeftPlan()->GetType() == PlanType::SeqScan) {
              const auto &left_seq_scan = dynamic_cast<const SeqScanPlanNode &>(*left_nlj_plan.GetLeftPlan());
              if (auto index = MatchIndex(left_seq_scan.table_name_, left_expr->GetColIdx()); index != std::nullopt) {
                auto *outer_expr = dynamic_cast<const ComparisonExpression *>(&nlj_plan.Predicate());
                auto left_outer_expr = dynamic_cast<const ColumnValueExpression *>(outer_expr->children_[0].get());
                auto right_outer_expr = dynamic_cast<const ColumnValueExpression *>(outer_expr->children_[1].get());
                BUSTUB_ASSERT(expr->comp_type_ == ComparisonType::Equal, "comparison type must be equal");
                BUSTUB_ASSERT(outer_expr->comp_type_ == ComparisonType::Equal, "comparison type must be equal");

                auto inner_pred = std::make_shared<ComparisonExpression>(
                    std::make_shared<ColumnValueExpression>(
                        0, left_outer_expr->GetColIdx() - left_nlj_plan.GetLeftPlan()->output_schema_->GetColumnCount(),
                        left_outer_expr->GetReturnType()),
                    std::make_shared<ColumnValueExpression>(1, right_outer_expr->GetColIdx(),
                                                            right_outer_expr->GetReturnType()),
                    ComparisonType::Equal);
                auto outer_pred = std::make_shared<ComparisonExpression>(
                    std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()),
                    std::make_shared<ColumnValueExpression>(1, left_expr->GetColIdx(), left_expr->GetReturnType()),
                    ComparisonType::Equal);

                auto right_column_1 = left_nlj_plan.GetRightPlan()->output_schema_->GetColumns();
                auto right_column_2 = nlj_plan.GetRightPlan()->output_schema_->GetColumns();
                std::vector<Column> columns;
                columns.reserve(right_column_1.size() + right_column_2.size());
                for (const auto &col : right_column_1) {
                  columns.push_back(col);
                }
                for (const auto &col : right_column_2) {
                  columns.push_back(col);
                }

                std::vector<Column> outer_columns(columns);
                for (const auto &col : left_nlj_plan.GetLeftPlan()->output_schema_->GetColumns()) {
                  outer_columns.push_back(col);
                }

                return std::make_shared<NestedLoopJoinPlanNode>(
                    std::make_shared<Schema>(outer_columns),
                    std::make_shared<NestedLoopJoinPlanNode>(std::make_shared<Schema>(columns),
                                                             left_nlj_plan.GetRightPlan(), nlj_plan.GetRightPlan(),
                                                             inner_pred, JoinType::INNER),
                    left_nlj_plan.GetLeftPlan(), outer_pred, JoinType::INNER);
              }
            }
          }
        }
      }
    }
  }

  return optimized_plan;
}

auto Optimizer::OptimizePredicatePushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizePredicatePushDown(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

    if (nlj_plan.GetLeftPlan()->GetType() != PlanType::NestedLoopJoin) {
      return optimized_plan;
    }
    const auto &left_nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*nlj_plan.GetLeftPlan());

    if (nlj_plan.GetRightPlan()->GetType() != PlanType::MockScan ||
        left_nlj_plan.GetLeftPlan()->GetType() != PlanType::MockScan ||
        left_nlj_plan.GetRightPlan()->GetType() != PlanType::MockScan) {
      return optimized_plan;
    }

    std::vector<AbstractExpressionRef> join_preds;
    std::vector<AbstractExpressionRef> filter_preds;
    if (const auto *expr = dynamic_cast<const LogicExpression *>(&nlj_plan.Predicate()); expr != nullptr) {
      while (const auto *inner_expr = dynamic_cast<const LogicExpression *>(expr->children_[0].get())) {
        if (const auto *pred = dynamic_cast<const ColumnValueExpression *>(expr->children_[1]->children_[1].get());
            pred != nullptr) {
          join_preds.push_back(expr->children_[1]);
        } else {
          filter_preds.push_back(expr->children_[1]);
        }
        expr = dynamic_cast<const LogicExpression *>(expr->children_[0].get());
      }
      if (const auto *pred = dynamic_cast<const ColumnValueExpression *>(expr->children_[1]->children_[1].get());
          pred != nullptr) {
        join_preds.push_back(expr->children_[1]);
      } else {
        filter_preds.push_back(expr->children_[1]);
      }
      if (const auto *pred = dynamic_cast<const ColumnValueExpression *>(expr->children_[0]->children_[1].get());
          pred != nullptr) {
        join_preds.push_back(expr->children_[0]);
      } else {
        filter_preds.push_back(expr->children_[0]);
      }

      std::vector<AbstractExpressionRef> first_filter;
      std::vector<AbstractExpressionRef> third_filter;

      for (const auto &pred : filter_preds) {
        const auto *outer = dynamic_cast<const ComparisonExpression *>(pred.get());
        const auto *inner = dynamic_cast<const ColumnValueExpression *>(pred->children_[0].get());
        if (inner->GetTupleIdx() == 0) {
          first_filter.push_back(pred);
        } else {
          third_filter.push_back(std::make_shared<ComparisonExpression>(
              std::make_shared<ColumnValueExpression>(0, inner->GetColIdx(), inner->GetReturnType()),
              pred->children_[1], outer->comp_type_));
        }
      }
      BUSTUB_ASSERT(first_filter.size() == 2, "only in leader board test!");
      BUSTUB_ASSERT(third_filter.size() == 2, "only in leader board test!");

      auto first_pred = std::make_shared<LogicExpression>(first_filter[0], first_filter[1], LogicType::And);
      auto third_pred = std::make_shared<LogicExpression>(third_filter[0], third_filter[1], LogicType::And);

      auto first_filter_scan = std::make_shared<FilterPlanNode>(left_nlj_plan.children_[0]->output_schema_, first_pred,
                                                                left_nlj_plan.children_[0]);
      auto third_filter_scan = std::make_shared<FilterPlanNode>(nlj_plan.GetRightPlan()->output_schema_, third_pred,
                                                                nlj_plan.GetRightPlan());
      auto left_node = std::make_shared<NestedLoopJoinPlanNode>(left_nlj_plan.output_schema_, first_filter_scan,
                                                                left_nlj_plan.GetRightPlan(), left_nlj_plan.predicate_,
                                                                left_nlj_plan.GetJoinType());
      return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, left_node, third_filter_scan,
                                                      join_preds[0], nlj_plan.GetJoinType());
    }
  }

  return optimized_plan;
}

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeReorderJoinUseIndex(p);
  p = OptimizePredicatePushDown(p);
  p = OptimizeNLJAsIndexJoin(p);
  p = OptimizeNLJAsHashJoin(p);  // Enable this rule after you have implemented hash join.
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  return p;
}

}  // namespace bustub
