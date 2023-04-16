#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      txn_(exec_ctx->GetTransaction()) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  auto comp = [order_bys = plan_->GetOrderBy(),
               schema = child_executor_->GetOutputSchema()] (const Tuple &tuple1, const Tuple &tuple2) {

        for (const auto &order_by : order_bys) {
          auto order_by_type = order_by.first;

          auto comp_t1 = order_by.second->Evaluate(&tuple1, schema);
          auto comp_t2 = order_by.second->Evaluate(&tuple2, schema);

          switch (order_by_type) {
            case OrderByType::INVALID:
              throw Exception(ExceptionType::INCOMPATIBLE_TYPE, "Invalid order by type!");
            case OrderByType::DEFAULT:
            case OrderByType::ASC:
              if (static_cast<bool>(comp_t1.CompareLessThan(comp_t2))) {
                return true;
              }
              if (static_cast<bool>(comp_t1.CompareGreaterThan(comp_t2))) {
                return false;
              }
              break;
            case OrderByType::DESC:
              if (static_cast<bool>(comp_t1.CompareGreaterThan(comp_t2))) {
                return true;
              }
              if (static_cast<bool>(comp_t1.CompareLessThan(comp_t2))) {
                return false;
              }
              break;
          }
        }
      };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(comp)> pq(comp);

  Tuple tuple;
  RID rid;
  while(child_executor_->Next(&tuple, &rid)) {
    pq.push(tuple);
    if(pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }

  while(!pq.empty()) {
    top_n_vtr_.emplace_back(pq.top());
    pq.pop();
  }
  itr_ = top_n_vtr_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(itr_ == top_n_vtr_.end()) {
    return false;
  }

  *tuple = *itr_++;
  *rid = tuple->GetRid();

  return true;
}

}  // namespace bustub
