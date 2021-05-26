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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  // TableMetadata *meta_table = exec_ctx->GetCatalog()->GetTable(plan->GetTableOid());
  // meta_table
}

void SeqScanExecutor::Init() {}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) { return false; }

}  // namespace bustub
