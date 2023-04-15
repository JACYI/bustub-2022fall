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
      right_executor_(std::move(right_executor)),
      txn_(exec_ctx->GetTransaction()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;

  while(right_executor_->Next(&tuple, &rid)) {
    right_tuples_.emplace_back(tuple);
  }
  right_tuple_idx_ = -1; // -1 represent current left_tuple_ is done for join
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();
  RID tmp;
  while(right_tuple_idx_ >= 0 || left_executor_->Next(&left_tuple_, &tmp)){
    if(right_tuple_idx_ == -1){
      right_tuple_idx_ = 0;
    }
    std::vector<Value> values;
    for(size_t right_idx = right_tuple_idx_; right_idx < right_tuples_.size(); ++right_idx) {
      // match success
      if(plan_->predicate_->EvaluateJoin(&left_tuple_, left_schema,
                                          &right_tuples_[right_idx], right_schema).GetAs<bool>()) {
        std::vector<Value> join_result_values{};
        for(size_t idx = 0; idx < left_schema.GetColumnCount(); ++idx) {
          join_result_values.emplace_back(left_tuple_.GetValue(&left_schema, idx));
        }
        for(size_t idx = 0; idx < right_schema.GetColumnCount(); ++idx) {
          join_result_values.emplace_back(right_tuples_[right_idx].GetValue(&right_schema, idx));
        }

        *tuple = {values, &GetOutputSchema()};
        right_tuple_idx_ = right_idx + 1;
        return true;
      }
    }


    // match failure, but left join and left_tuple_ does not match with any right tuples
    if(plan_->GetJoinType() == JoinType::LEFT && right_tuple_idx_ == -1) {
      std::vector<Value> join_result_values{};
      for(size_t idx = 0; idx < left_schema.GetColumnCount(); ++idx) {
        join_result_values.emplace_back(left_tuple_.GetValue(&left_schema, idx));
      }
      for(size_t idx = 0; idx < right_schema.GetColumnCount(); ++idx) {
        auto tid = right_schema.GetColumn(idx).GetType();
        join_result_values.emplace_back(ValueFactory::GetNullValueByType(tid));
      }

      *tuple = {values, &GetOutputSchema()};
      return true;
    }
    right_tuple_idx_ = -1;
  }
  return false;
}

}  // namespace bustub
