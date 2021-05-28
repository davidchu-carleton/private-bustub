//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), 
    itr_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->Begin(exec_ctx->GetTransaction())) {
    meta_table_ = exec_ctx->GetCatalog()->GetTable(plan->GetTableOid());
    txn_ = exec_ctx->GetTransaction();
    itr_ = meta_table_->table_->Begin(txn_);
    child_exc_ = std::move(child_executor);

}

void InsertExecutor::Init() {}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    TableHeap *table = meta_table_->table_.get();
    //check is Raw insert
    if (plan_->IsRawInsert()) {
        std::vector<std::vector<Value>> raw_values = plan_->RawValues();
        for (std::vector<Value> raw_value : raw_values) {
            Tuple new_tuple = Tuple(raw_value, &meta_table_->schema_);
            RID new_rid = new_tuple.GetRid();
            table->InsertTuple(new_tuple, &new_rid, txn_);
        }
    } else {
        while (child_exc_->Next(tuple, rid)) {
            table->InsertTuple(*tuple, rid, txn_);
        }
    }
    return false; 
}

}  // namespace bustub
