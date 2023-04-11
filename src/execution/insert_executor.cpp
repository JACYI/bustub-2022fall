//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  this->exec_ctx_ = exec_ctx;
  this->plan_ = plan;
  this->child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  child_executor_->Init();
  this->tbl_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  this->idx_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(tbl_info_->name_);
  this->txn_ = exec_ctx_->GetTransaction();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(child_executor_ == nullptr || tbl_info_ == nullptr) {
    return false;
  }
  // values to be inserted from a child exector (for example, INSERT INTO ... SELECT)
  int32_t count{0};
  while(child_executor_->Next(tuple, rid)) {
    bool is_inserted = tbl_info_->table_->InsertTuple(*tuple, rid, txn_);
    if(is_inserted) {
      // update indexes info
      for(auto idx_info : idx_infos_) {
        idx_info->index_->InsertEntry(*tuple, *rid, txn_);
      }
      count++;
    }
  }
  // return the Tuple with inserted rows number
  auto s = GetOutputSchema();
  std::vector<Value> values;
  values.reserve(s.GetColumnCount());
  values.emplace_back(TypeId::INTEGER, count);
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub
