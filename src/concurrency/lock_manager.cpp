//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // table locking should support all lock modes

  // compatibility check, some locks limited for some isolution levels or some stages
  if(txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // read uncommitted level not requires S lock (S/IS/SIX)
    if(lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  } else if(txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && txn->GetState() == TransactionState::SHRINKING) {
    // RC level not requires X lock in the shrinking stage
    if(lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    // no locks are required at shrinking stage when isolation level is RR
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  // get global map latch
  table_lock_map_latch_.lock();
  if(table_lock_map_.count(oid) == 0) {
    // init queue
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  // get the request queue lock and then release the global lock
  auto lock_request_queue = table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  // lock upgrade/duplicate check
  for(const auto& lock_request : lock_request_queue->request_queue_) {
    if(lock_request->txn_id_ != txn->GetTransactionId()){
      continue ;
    }

    // duplicate lock request
    if(lock_request->lock_mode_ == lock_mode){
      lock_request_queue->latch_.unlock();
      return true;
    }

    // upgrade
    // only one transaction should be allowed to upgrade its lock on a table
    if(lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }

    // upgrade check
    switch (lock_mode) {
      case LockMode::EXCLUSIVE:
        // X lock can not be upgraded
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        break ;

      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        if(lock_mode != LockMode::EXCLUSIVE) {
          // SIX lock can not be upgraded for except Xlock
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        }

      case LockMode::INTENTION_EXCLUSIVE:
      case LockMode::SHARED:
        if(lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
          // IX, S lock can not be upgraded for except X, SIX lock
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        }

      case LockMode::INTENTION_SHARED:
        // IS lock can be upgraded without limited
        break ;
    }

    // lock upgrade
    lock_request_queue->request_queue_.remove(lock_request);
    auto upgraded_lock_request = std::make_shared<LockRequest>(
        txn->GetTransactionId(), lock_mode, oid);
    // upgraded lock request should be prioritised over other waiting lock requests
    auto insert_itr = std::find_if(lock_request_queue->request_queue_.begin(),
                                   lock_request_queue->request_queue_.end(),
                                   [](const std::shared_ptr<LockRequest>& lr) -> bool {
                                     // find first element which is not granted
                                     return !lr->granted_;
                                   });
    lock_request_queue->request_queue_.insert(insert_itr, upgraded_lock_request);
    // upgrade the upgrading_ record before acquired lock
    lock_request_queue->upgrading_ = txn->GetTransactionId();

    // try to grant lock for first lock which is not be granted in request queue
    std::unique_lock<std::mutex> lk(lock_request_queue->latch_, std::adopt_lock);
    while(!GrantLock(upgraded_lock_request, lock_request_queue)) {
      // lock request not allowed, should be deleted
      lock_request_queue->cv_.wait(lk);
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      lock_request_queue->request_queue_.remove(upgraded_lock_request);

      return false;
    }
  }


}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { return true; }

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // row locking should not supported intention locks
  if(lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED
      || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return false;
  }
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { return true; }

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

auto LockManager::GrantLock(const std::shared_ptr<LockRequest>& lock_request, const std::shared_ptr<LockRequestQueue>& lock_request_queue) -> bool {
  for(const auto& lr : lock_request_queue->request_queue_) {
    if(!lr->granted_) {
      continue ;
    }

    // accroding to the conflict matrix
    switch (lr->lock_mode_) {
      case LockMode::EXCLUSIVE:
        return false;

      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        if(lock_request->lock_mode_ != LockMode::SHARED_INTENTION_EXCLUSIVE) {
          return false;
        }

      case LockMode::SHARED:
        if(lock_request->lock_mode_ != LockMode::SHARED && lock_request->lock_mode_ != LockMode::INTENTION_SHARED) {
          return false;
        }

      case LockMode::INTENTION_EXCLUSIVE:
        if(lock_request->lock_mode_ != LockMode::INTENTION_EXCLUSIVE && lock_request->lock_mode_ != LockMode::INTENTION_SHARED) {
          return false;
        }

      case LockMode::INTENTION_SHARED:
        if(lock_request->lock_mode_ == LockMode::EXCLUSIVE) {
          return false;
        }
    }

  }

  // new lock request has no conflict with granted locks
  return true;
}

}  // namespace bustub
