#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  txn_ = exec_ctx->GetTransaction();
}



void SortExecutor::Init() {
  Tuple tuple;
  RID rid;
  while(child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }

  auto sorted_keys = plan_->GetOrderBy();
  std::sort(tuples_.begin(), tuples_.end(), [order_bys = plan_->GetOrderBy(),
                                           schema = child_executor_->GetOutputSchema()]
            (const Tuple &tuple1, const Tuple &tuple2){

              for(const auto& order_by : order_bys) {
                auto order_by_type = order_by.first;

                auto comp_t1 = order_by.second->Evaluate(&tuple1, schema);
                auto comp_t2 = order_by.second->Evaluate(&tuple2, schema);

                switch(order_by_type) {
                  case OrderByType::INVALID:
                    throw Exception(ExceptionType::INCOMPATIBLE_TYPE, "Invalid order by type!");
                  case OrderByType::DEFAULT:
                  case OrderByType::ASC:
                    if(static_cast<bool>(comp_t1.CompareLessThan(comp_t2))) {
                      return true;
                    }
                    if(static_cast<bool>(comp_t1.CompareGreaterThan(comp_t2))) {
                      return false;
                    }
                    break ;
                  case OrderByType::DESC:
                    if(static_cast<bool>(comp_t1.CompareGreaterThan(comp_t2))) {
                      return true;
                    }
                    if(static_cast<bool>(comp_t1.CompareLessThan(comp_t2))) {
                      return false;
                    }
                    break;
                }
              }
    return false;
  });

  itr_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(itr_ == tuples_.end()) {
    return false;
  }

  *tuple = *itr_++;
  *rid = tuple->GetRid();

  return true;
}

}  // namespace bustub
