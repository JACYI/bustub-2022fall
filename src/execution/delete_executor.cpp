//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  txn_ = exec_ctx->GetTransaction();
}

void DeleteExecutor::Init() {
  tbl_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  idx_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(tbl_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  int32_t count{0};
  while(child_executor_->Next(tuple, rid)) {
    bool is_deleted = tbl_info_->table_->MarkDelete(*rid, txn_);
    if(is_deleted) {
      // update the index
      for(auto idx_info : idx_infos_) {
        auto idx_key = tuple->KeyFromTuple(
            tbl_info_->schema_, idx_info->key_schema_, idx_info->index_->GetKeyAttrs());
        idx_info->index_->DeleteEntry(idx_key, *rid, txn_);
      }
      count++;
    }
  }

  // return the deleted rows number
  std::vector<Value> v;
  v.emplace_back(Value(TypeId::INTEGER, count));
  *tuple = Tuple(v, &tbl_info_->schema_);
  return true;
}

}  // namespace bustub
