//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      itr_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->Begin(exec_ctx->GetTransaction())) {
  meta_table_ = exec_ctx->GetCatalog()->GetTable(plan->GetTableOid());
  txn_ = exec_ctx->GetTransaction();
  itr_ = meta_table_->table_->Begin(txn_);
}

void SeqScanExecutor::Init() {}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (itr_ == meta_table_->table_->End()) {  // no more tuples left
    return false;
  }
  // if (plan_->GetPredicate() == nullptr) { //no predicate
  //     *tuple = *itr_;
  //     *rid = tuple->GetRid();
  //     itr_++;
  //     return true;
  // }
  while (plan_->GetPredicate() != nullptr &&
         !plan_->GetPredicate()->Evaluate(&(*itr_), &meta_table_->schema_).GetAs<bool>()) {
    itr_++;
    if (itr_ == meta_table_->table_->End()) {  // no more tuples that satisfy the predicate
      return false;
    }
  }
  *tuple = *itr_;
  *rid = tuple->GetRid();
  itr_++;
  return true;
}

}  // namespace bustub
