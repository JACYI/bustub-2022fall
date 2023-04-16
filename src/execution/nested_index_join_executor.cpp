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
      exec_ctx_(exec_ctx),
      child_executor_(std::move(child_executor)),
      txn_(exec_ctx->GetTransaction()){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  auto idx_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(idx_info->index_.get());

  idx_info = exec_ctx->GetCatalog()->GetIndex(plan->index_oid_);
  inner_tbl_info_ = exec_ctx->GetCatalog()->GetTable(plan->inner_table_oid_);
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple outer_tuple;
  RID outer_rid;
  std::vector<Value> values;

  while(child_executor_->Next(&outer_tuple, &outer_rid)) {
    auto value = plan_->KeyPredicate()->Evaluate(&outer_tuple, child_executor_->GetOutputSchema());
    std::vector<RID> rids;
    tree_->ScanKey(Tuple{{value}, idx_info_->index_->GetKeySchema()}, &rids, txn_);

    if(!rids.empty()){
      Tuple inner_tuple;
      inner_tbl_info_->table_->GetTuple(rids[0], &inner_tuple, txn_);
      for(size_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(outer_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }

      for(size_t i = 0; i < inner_tbl_info_->schema_.GetColumnCount(); ++i) {
        values.emplace_back(inner_tuple.GetValue(&inner_tbl_info_->schema_, i));
      }

      *tuple = {values, &GetOutputSchema()};
      return true;
    }

    if(plan_->GetJoinType() == JoinType::LEFT) {
      for(size_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.emplace_back(outer_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }

      auto inner_schema = inner_tbl_info_->schema_;
      for(size_t i = 0; i < inner_schema.GetColumnCount(); ++i) {
        auto tid = inner_schema.GetColumn(i).GetType();
        values.emplace_back(ValueFactory::GetNullValueByType(tid));
      }

      *tuple = {values, &GetOutputSchema()};
      return true;
    }

  }

  return false;
}

}  // namespace bustub
