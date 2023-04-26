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
  CheckLockCompatibility(txn, lock_mode);

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
    CheckUpgradeCompatibility(txn, lock_mode);

    // lock upgrade
    lock_request_queue->request_queue_.remove(lock_request);
    DeleteTableLockRecord(txn, lock_request);
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
//      if(txn->GetState() == TransactionState::ABORTED) {
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        lock_request_queue->request_queue_.remove(upgraded_lock_request);
        lock_request_queue->cv_.notify_all();
        return false;
//      }
    }

    // grant lock
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    upgraded_lock_request->granted_ = true;
    // lock record
    InsertTableLockRecord(txn, upgraded_lock_request);

    // TODO: why notification all requests
    if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
    }
    return true;
  }

  // simple lock
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(lock_request);

  std::unique_lock<std::mutex> lk(lock_request_queue->latch_, std::adopt_lock);
  while(!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lk);
    if(txn->GetState() == TransactionState::ABORTED) {
        lock_request_queue->request_queue_.remove(lock_request);
        lock_request_queue->cv_.notify_all();
        return false;
    }
  }

  lock_request->granted_ = true;
  InsertTableLockRecord(txn, lock_request);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  if(table_lock_map_.count(oid) == 0) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  auto s_row_lock_set = txn->GetSharedRowLockSet();

  // unlocking a table should only be allowed if the transaction does not hold locks on any row on that table
  if((x_row_lock_set->count(oid) != 0 || x_row_lock_set->find(oid)->second.empty()) ||
      (s_row_lock_set->count(oid) != 0 || s_row_lock_set->find(oid)->second.empty())) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  // transaction state update

  auto lock_request = table_lock_map_.find(oid)->second;
  lock_request->latch_.lock();
  table_lock_map_latch_.unlock();

  for(const auto& lr : lock_request->request_queue_) {
    if(lr->txn_id_ == txn->GetTransactionId() && lr->granted_) {

        // unlock
        lr->granted_ = false;
        lock_request->request_queue_.remove(lr);
        DeleteTableLockRecord(txn, lr);


        // transaction state update
        TransacntionStateUpdate(txn, lr);

        // notify other requests
        lock_request->cv_.notify_all();
        lock_request->latch_.unlock();
        return true;
    }
  }

  // transaction does not hold lock
  lock_request->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // row locking should not support intention locks
  if(lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED
      || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  CheckLockCompatibility(txn, lock_mode);

  // multilevel locking
  MultilevelLockCheck(txn, oid, lock_mode);

  row_lock_map_latch_.lock();
  if(row_lock_map_.count(rid) == 0) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto row_lock_request = row_lock_map_.find(rid)->second;
  row_lock_request->latch_.lock();
  row_lock_map_latch_.unlock();

  for(const auto &lock_request : row_lock_request->request_queue_) {
    if(lock_request->txn_id_ != txn->GetTransactionId()) {
        continue ;
    }

    if(lock_request->lock_mode_ == lock_mode) {
        row_lock_request->latch_.unlock();
        return true;
    }

    // lock upgrade check
    if(row_lock_request->upgrading_ != INVALID_TXN_ID) {
        row_lock_request->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    // upgrade check
    CheckUpgradeCompatibility(txn, lock_mode);

    // upgrade lock
    row_lock_request->request_queue_.remove(lock_request);
    DeleteRowLockRecord(txn, lock_request);

    auto upgraded_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    auto insert_itr = std::find_if(row_lock_request->request_queue_.begin(),
                                   row_lock_request->request_queue_.end(),
                                   [](const std::shared_ptr<LockRequest>& lr) -> bool {
                                     // find first element which is not granted
                                     return !lr->granted_;
                                   });
    row_lock_request->request_queue_.insert(insert_itr, upgraded_lock_request);
    // upgrade the upgrading_ record before acquired lock
    row_lock_request->upgrading_ = txn->GetTransactionId();

    // grant lock check
    std::unique_lock<std::mutex> lk(row_lock_request->latch_, std::adopt_lock);
    while(!GrantLock(upgraded_lock_request, row_lock_request)) {
        row_lock_request->cv_.wait(lk);
        if (txn->GetState() == TransactionState::ABORTED) {
          row_lock_request->upgrading_ = INVALID_TXN_ID;
          row_lock_request->request_queue_.remove(upgraded_lock_request);
          row_lock_request->cv_.notify_all();
          return false;
        }
    }

    // grant lock
    upgraded_lock_request->granted_ = true;
    row_lock_request->upgrading_ = INVALID_TXN_ID;
    InsertRowLockRecord(txn, upgraded_lock_request);

    if (lock_mode != LockMode::EXCLUSIVE) {
        row_lock_request->cv_.notify_all();
    }

    return true;
  }

  // new lock request
  auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  row_lock_request->request_queue_.push_back(new_lock_request);

  // grant lock check
  std::unique_lock<std::mutex> lk(row_lock_request->latch_, std::adopt_lock);
  while(!GrantLock(new_lock_request, row_lock_request)) {
    row_lock_request->cv_.wait(lk);
    if (txn->GetState() == TransactionState::ABORTED) {
        row_lock_request->upgrading_ = INVALID_TXN_ID;
        row_lock_request->request_queue_.remove(new_lock_request);
        row_lock_request->cv_.notify_all();
        return false;
    }
  }

  // grant lock
  new_lock_request->granted_ = true;
  InsertRowLockRecord(txn, new_lock_request);

  if (lock_mode != LockMode::EXCLUSIVE) {
    row_lock_request->cv_.notify_all();
  }

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  if(row_lock_map_.count(rid) == 0) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto row_lock_request_queue = row_lock_map_.find(rid)->second;
  row_lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  for(const auto &lr : row_lock_request_queue->request_queue_) {
    if(lr->txn_id_ != txn->GetTransactionId() || !lr->granted_) {
        continue ;
    }

    // unlock
    row_lock_request_queue->request_queue_.remove(lr);
    DeleteRowLockRecord(txn, lr);

    // delete records
    TransacntionStateUpdate(txn, lr);

    // notify others waiting for the lock
    row_lock_request_queue->cv_.notify_all();
    row_lock_request_queue->latch_.unlock();
    return true;

  }

  // not found locked request
  row_lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

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

void LockManager::CheckLockCompatibility(Transaction *txn, const LockMode &lock_mode) {
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
}

void LockManager::CheckUpgradeCompatibility(Transaction *txn, const LockMode &lock_mode) {
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
}

void LockManager::InsertTableLockRecord(Transaction *txn, const std::shared_ptr<LockRequest>& lock_request) {
  switch (lock_request->lock_mode_) {
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(lock_request->oid_);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(lock_request->oid_);
      break;
  }
}

void LockManager::DeleteTableLockRecord(Transaction *txn, const std::shared_ptr<LockRequest>& lock_request) {
  switch (lock_request->lock_mode_) {
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(lock_request->oid_);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(lock_request->oid_);
      break;
  }
}

void LockManager::InsertRowLockRecord(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) {
  auto x_lock_map = txn->GetExclusiveRowLockSet();
  auto s_lock_map = txn->GetSharedRowLockSet();
  switch (lock_request->lock_mode_) {
    case LockMode::EXCLUSIVE:
      if(x_lock_map->count(lock_request->oid_) == 0) {
        x_lock_map->emplace(lock_request->oid_, std::unordered_set<RID>{});
      }
      x_lock_map->find(lock_request->oid_)->second.emplace(lock_request->rid_);
      break;
    case LockMode::SHARED:
      if(s_lock_map->count(lock_request->oid_) == 0) {
        s_lock_map->emplace(lock_request->oid_, std::unordered_set<RID>{});
      }
      s_lock_map->find(lock_request->oid_)->second.emplace(lock_request->rid_);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::INTENTION_SHARED:
      break ;
  }
}

void LockManager::DeleteRowLockRecord(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request) {
  auto x_lock_map = txn->GetExclusiveRowLockSet();
  auto s_lock_map = txn->GetSharedRowLockSet();
  switch (lock_request->lock_mode_) {
    case LockMode::EXCLUSIVE:
      x_lock_map->find(lock_request->oid_)->second.erase(lock_request->rid_);
      break;
    case LockMode::SHARED:
      s_lock_map->find(lock_request->oid_)->second.erase(lock_request->rid_);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::INTENTION_SHARED:
      break ;
  }
}

void LockManager::TransacntionStateUpdate(Transaction *txn, const std::shared_ptr<LockRequest> &lr) {
  if(lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
      lr->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
      lr->lock_mode_ == LockMode::INTENTION_SHARED) {
    // only unlocking X or S locks changes the state
    return ;
  }
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      // Unlocking S/X locks should set the transaction state to SHRINKING
      txn->SetState(TransactionState::SHRINKING);
    case IsolationLevel::READ_COMMITTED:
      if(lr->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    case IsolationLevel::READ_UNCOMMITTED:
      // S locks are undefined in this level
      txn->SetState(TransactionState::SHRINKING);
  }
}

void LockManager::MultilevelLockCheck(Transaction *txn, table_oid_t oid, LockMode lock_mode) {
  if(lock_mode == LockMode::SHARED) {
    if(txn->GetSharedTableLockSet()->count(oid) == 0 &&
        txn->GetIntentionSharedTableLockSet()->count(oid) == 0 &&
        txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) == 0) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  } else {
    if(txn->GetExclusiveTableLockSet()->count(oid) == 0 &&
        txn->GetIntentionExclusiveTableLockSet()->count(oid) == 0 &&
        txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) == 0) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
}

}  // namespace bustub
