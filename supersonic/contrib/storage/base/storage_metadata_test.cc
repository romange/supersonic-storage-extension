// Copyright 2014 Google Inc.  All Rights Reserved
// Author: Wojtek Żółtak (wojciech.zoltak@gmail.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Classes for file-based persistent storage. Currently, only the unix file
// system is supported. It is possible to integrate other file systems,
// e.g. distributed FS like HDFS, by creating an implementation of File and
// PathUtil interfaces.
//
// No two storage objects for single location should be active at the same time
// (either Writable or Readable).

#include "supersonic/contrib/storage/base/storage_metadata.h"

#include "gtest/gtest.h"

#include "supersonic/base/infrastructure/tuple_schema.h"


namespace supersonic {
namespace {

const PageMetadata& GetPageMetadata(uint64_t page_number,
                                    rowcount_t row_count) {
  PageMetadata metadata;
  metadata.set_page_number(page_number);
  metadata.set_row_count(row_count);
  return metadata;
}


TEST(StorageMetadataTest, DeSerializationWorks) {
  uint32_t schema_a_family = 1;
  TupleSchema schema_a;
  schema_a.add_attribute(Attribute("A", INT32, NULLABLE));

  uint32_t schema_b_family = 31;
  TupleSchema schema_b;
  schema_b.add_attribute(Attribute("B", DOUBLE, NOT_NULLABLE));

  std::vector<Family> partitioned_schema;
  partitioned_schema.emplace_back(schema_a_family, schema_a);
  partitioned_schema.emplace_back(schema_b_family, schema_b);

  auto metadata_writer = CreateMetadataWriter(partitioned_schema,
                                              HeapBufferAllocator::Get());
  ASSERT_TRUE(metadata_writer.is_success());

  metadata_writer->AppendPage(schema_a_family, GetPageMetadata(8, 10));
  metadata_writer->AppendPage(schema_a_family, GetPageMetadata(5, 10));
  metadata_writer->AppendPage(schema_b_family, GetPageMetadata(2, 30));

  auto metadata_page = metadata_writer->DumpToPage();
  ASSERT_TRUE(metadata_page.is_success());
  auto metadata_result = ReadStorageMetadata(*metadata_page);
  ASSERT_TRUE(metadata_result.is_success());
  std::unique_ptr<StorageMetadata> metadata(metadata_result.release());

  ASSERT_EQ(2, metadata->page_families_size());
  for (int i = 0; i < metadata->page_families_size(); i++) {
    const PageFamily& family = metadata->page_families(i);
    ASSERT_EQ(partitioned_schema[i].first,
              family.family_number());

    auto tuple_schema =
        SchemaConverter::SchemaProtoToTupleSchema(family.schema());
    ASSERT_TRUE(tuple_schema.is_success());
    TupleSchema::AreEqual(partitioned_schema[i].second,
                          tuple_schema.get(),
                          true /* check names */);
  }

  PageMetadata page1 = metadata->page_families(0).pages(0);
  PageMetadata page2 = metadata->page_families(0).pages(1);
  PageMetadata page3 = metadata->page_families(1).pages(0);
  ASSERT_EQ(8, page1.page_number());
  ASSERT_EQ(10, page1.row_count());
  ASSERT_EQ(5, page2.page_number());
  ASSERT_EQ(10, page2.row_count());
  ASSERT_EQ(2, page3.page_number());
  ASSERT_EQ(30, page3.row_count());
}

}  // namespace
}  // namespace supersonic
