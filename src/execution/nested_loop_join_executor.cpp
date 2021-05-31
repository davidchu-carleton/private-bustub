//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <vector>

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_exc_(std::move(left_executor)),
      right_exc_(std::move(right_executor)) {
  catalog_ = exec_ctx->GetCatalog();
}

void NestedLoopJoinExecutor::Init() {
  // get left and right tuple
  Tuple left_tuple = Tuple();
  RID left_rid = RID();
  Tuple right_tuple = Tuple();
  RID right_rid = RID();
  // create a vector of right tuples
  std::vector<Tuple> right_tuple_vector;
  while (right_exc_->Next(&right_tuple, &right_rid)) {
    right_tuple_vector.emplace_back(right_tuple);
  }
  while (left_exc_->Next(&left_tuple, &left_rid)) {
    for (Tuple &right_tuple : right_tuple_vector) {
      bool predicate_check =
          plan_->Predicate()
              ->EvaluateJoin(&left_tuple, left_exc_->GetOutputSchema(), &right_tuple, right_exc_->GetOutputSchema())
              .GetAs<bool>();
      if (predicate_check) {  // if there is a match
        // make a new Value vector by combining the values from left and right
        uint32_t size_left = left_exc_->GetOutputSchema()->GetColumnCount();
        uint32_t size_right = right_exc_->GetOutputSchema()->GetColumnCount();
        std::vector<Value> value_vector;
        for (uint32_t i = 0; i < size_left; i++) {
          Value left_value = left_tuple.GetValue(left_exc_->GetOutputSchema(), i);
          value_vector.emplace_back(left_value);
        }
        for (uint32_t i = 0; i < size_right; i++) {
          Value right_value = right_tuple.GetValue(right_exc_->GetOutputSchema(), i);
          value_vector.emplace_back(right_value);
        }
        Tuple combined_tuple = Tuple(value_vector, plan_->OutputSchema());
        tuples_.emplace_back(combined_tuple);
      }
    }
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (tuples_.empty()) {
    return false;
  }
  // return the least recently added tuple
  Tuple res = tuples_.front();
  *tuple = res;
  *rid = res.GetRid();
  tuples_.erase(tuples_.begin());
  return true;
}

}  // namespace bustub
