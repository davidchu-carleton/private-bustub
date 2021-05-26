//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "gtest/gtest.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(CatalogTest, CreateTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::BIGINT);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  Transaction txn(0);
  auto *table_metadata = catalog->CreateTable(&txn, table_name, schema);

  EXPECT_EQ(table_name, table_metadata->name_);
  EXPECT_EQ(schema.ToString(), table_metadata->schema_.ToString());
  EXPECT_EQ(0, table_metadata->oid_);
  EXPECT_EQ(1, table_metadata->table_->GetFirstPageId());

  EXPECT_EQ(table_metadata, catalog->GetTable(table_name));
  EXPECT_EQ(table_metadata, catalog->GetTable(0));

  std::vector<RID> rids;
  for (int i = 1; i < 6; i++) {
    RID rid;
    std::vector<Value> vals{Value(TypeId::BIGINT, i), Value(TypeId::BOOLEAN, static_cast<int32_t>((i % 2) == 0))};
    table_metadata->table_->InsertTuple(Tuple(vals, &schema), &rid, &txn);
    rids.emplace_back(rid);
  }

  table_metadata = catalog->CreateTable(&txn, "foo", schema);

  EXPECT_EQ(1, table_metadata->oid_);

  delete catalog;
  delete bpm;
  delete disk_manager;
}

TEST(CatalogTest, DISABLED_CreateIndexTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::BIGINT);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  Transaction txn(0);
  auto *table_metadata = catalog->CreateTable(&txn, table_name, schema);

  EXPECT_EQ(table_name, table_metadata->name_);
  EXPECT_EQ(schema.ToString(), table_metadata->schema_.ToString());
  EXPECT_EQ(0, table_metadata->oid_);
  EXPECT_EQ(1, table_metadata->table_->GetFirstPageId());

  EXPECT_EQ(table_metadata, catalog->GetTable(table_name));
  EXPECT_EQ(table_metadata, catalog->GetTable(0));

  std::vector<RID> rids;
  for (int i = 1; i < 6; i++) {
    RID rid;
    std::vector<Value> vals{Value(TypeId::BIGINT, i), Value(TypeId::BOOLEAN, static_cast<int32_t>((i % 2) == 0))};
    table_metadata->table_->InsertTuple(Tuple(vals, &schema), &rid, &txn);
    rids.emplace_back(rid);
  }

  std::string index_name = "idx_" + table_name;
  EXPECT_THROW(catalog->GetIndex(table_name, index_name), std::out_of_range);
  EXPECT_THROW(catalog->GetIndex("foo", "bar"), std::out_of_range);

  columns.clear();
  columns.emplace_back("A", TypeId::BIGINT);
  Schema key_schema(columns);
  auto *index_info = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(&txn, index_name, table_name,
                                                                                    schema, key_schema, {0}, 8);
  EXPECT_EQ(index_name, index_info->name_);
  EXPECT_EQ(table_name, index_info->table_name_);
  EXPECT_EQ(key_schema.ToString(), index_info->key_schema_.ToString());
  EXPECT_EQ(8, index_info->key_size_);
  EXPECT_EQ(0, index_info->index_oid_);
  IndexMetadata *meta = index_info->index_->GetMetadata();
  EXPECT_EQ(index_name, meta->GetName());
  EXPECT_EQ(table_name, meta->GetTableName());
  EXPECT_EQ(1, meta->GetIndexColumnCount());
  EXPECT_EQ(1, meta->GetKeyAttrs().size());
  EXPECT_EQ(0, meta->GetKeyAttrs()[0]);
  EXPECT_EQ(key_schema.ToString(), meta->GetKeySchema()->ToString());

  for (const RID &rid : rids) {
    Tuple t;
    std::vector<RID> result;
    table_metadata->table_->GetTuple(rid, &t, &txn);
    index_info->index_->ScanKey(t, &result, &txn);
    EXPECT_EQ(1, result.size());
    EXPECT_EQ(rid, result[0]);
  }

  table_metadata = catalog->CreateTable(&txn, "foo", schema);
  index_info = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(&txn, "idx_foo", "foo", schema,
                                                                              key_schema, {0}, 8);
  EXPECT_EQ(1, table_metadata->oid_);
  EXPECT_EQ(1, index_info->index_oid_);

  delete catalog;
  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
