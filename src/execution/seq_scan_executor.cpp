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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), 
    plan_(plan),
    itr_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->Begin(exec_ctx->GetTransaction()))   {
    meta_table_ = exec_ctx->GetCatalog()->GetTable(plan->GetTableOid());
    txn_ = exec_ctx->GetTransaction();
    itr_ = meta_table_->table_->Begin(txn_);
}

void SeqScanExecutor::Init() {}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) { 
    itr_++;
    if (itr_ == meta_table_->table_->End()) { //no more tuples left
        return false; 
    }
    while(plan_->GetPredicate()->Evaluate(&(*itr_), &meta_table_->schema_).GetAs<bool>()) {
        itr_++;
    }
    *tuple = *itr_;
    *rid = tuple->GetRid();
    return true;
}

}  // namespace bustub
