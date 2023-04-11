//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  this->exec_ctx_ = exec_ctx;
  this->plan_ = plan;
  this->tbl_info_ = exec_ctx->GetCatalog()->GetTable(plan->GetTableOid());
}

void SeqScanExecutor::Init() {
  this->tbl_itr_ = tbl_info_->table_->Begin(exec_ctx_->GetTransaction());
  this->end_ = tbl_info_->table_->End();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(tbl_itr_ == end_) {
    return false;
  }
  auto filter = plan_->filter_predicate_;
  do {
    if(tbl_itr_ == end_) {
      return false;
    }
    *tuple = *tbl_itr_;
    *rid = tuple->GetRid();
    ++tbl_itr_;
  } while (filter == nullptr || !filter->Evaluate(tuple, tbl_info_->schema_).GetAs<bool>());

  return true;
}

}  // namespace bustub
