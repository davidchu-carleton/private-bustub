//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

const Schema *AggregationExecutor::GetOutputSchema() { return plan_->OutputSchema(); }

void AggregationExecutor::Init() {
  this->child_->Init();
  Tuple tuple;
  RID rid;
  while (this->child_->Next(&tuple, &rid)) {
    auto key = this->MakeKey(&tuple);
    auto value = this->MakeVal(&tuple);
    this->aht_.InsertCombine(key, value);
  }
  this->aht_iterator_ = this->aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (this->aht_iterator_ != this->aht_.End()) {
    auto group_keys = this->aht_iterator_.Key().group_bys_;
    auto agg_vals = this->aht_iterator_.Val().aggregates_;
    if ((this->plan_->GetHaving() == nullptr) ||
        (this->plan_->GetHaving()->EvaluateAggregate(group_keys, agg_vals).GetAs<bool>())) {
      std::vector<Value> result;
      int count = plan_->OutputSchema()->GetColumnCount();
      for (int i=0; i < count; i++) {
        result.push_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->EvaluateAggregate(group_keys, agg_vals));
      }
      *tuple = Tuple(result, plan_->OutputSchema());
      ++aht_iterator_;
      return true;
    }
    ++this->aht_iterator_;
  }
  return false;
}

}  // namespace bustub
