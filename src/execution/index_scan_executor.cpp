//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  this->idx_info_ = exec_ctx->GetCatalog()->GetIndex(plan->index_oid_);
  this->tbl_info_ = exec_ctx->GetCatalog()->GetTable(idx_info_->table_name_);
  this->txn_ = exec_ctx->GetTransaction();
}

void IndexScanExecutor::Init() {
  this->tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(idx_info_->index_.get());
  if(plan_->filter_predicate_ == nullptr) {
    this->idx_itr_ = tree_->GetBeginIterator();
  } else {
    auto right_expr = dynamic_cast<ConstantValueExpression *>(plan_->filter_predicate_->get()->children_[1].get());
    Value value = right_expr->val_;
    std::vector<RID> *results{};
    tree_->ScanKey(Tuple{{value}, idx_info_->index_->GetKeySchema()}, results, txn_);
    if(results == nullptr) {
      return ;
    }
    this->vtr_itr_ = results->begin();
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(plan_->filter_predicate_ == nullptr) {
    *rid = (*idx_itr_).second;
    bool isSuccess = tbl_info_->table_->GetTuple(*rid, tuple, txn_, false);
    if(!isSuccess) {
      return false;
    }
    ++idx_itr_;
    return true;
  }
  *rid = *vtr_itr_;
  bool isSuccess = tbl_info_->table_->GetTuple(*rid, tuple, txn_, false);
  if(!isSuccess) {
    return false;
  }
  ++vtr_itr_;
  return true;
}

}  // namespace bustub
