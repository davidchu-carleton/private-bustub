//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * InsertExecutor executes an insert into a table.
 * Inserted values can either be embedded in the plan itself ("raw insert") or come from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new insert executor.
   * @param exec_ctx the executor context
   * @param plan the insert plan to be executed
   * @param child_executor the child executor to obtain insert values from, can be nullptr
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  void Init() override;

  // Note that Insert does not make use of the tuple pointer being passed in.
  // return false to indicate no more work to do when the
  // insert succeeds, and throw a std::runtime_error when it fails
  bool Next([[maybe_unused]] Tuple *tuple, RID *rid) override;

 private:
  /** The insert plan node to be executed. */
  const InsertPlanNode *plan_;
  TableIterator itr_;
  TableMetadata *meta_table_;
  Transaction *txn_;
  std::unique_ptr<AbstractExecutor> child_exc_;
};
}  // namespace bustub
