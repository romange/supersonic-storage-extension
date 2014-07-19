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

#include "supersonic/contrib/storage/core/schema_serialization.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace supersonic {
namespace {

TEST(SchemaSerializationTest, DumpThenReadSchema) {
  uint32_t schema_a_family = 1;
  TupleSchema schema_a;
  schema_a.add_attribute(Attribute("A", INT32, NULLABLE));

  uint32_t schema_b_family = 31;
  TupleSchema schema_b;
  schema_b.add_attribute(Attribute("B", DOUBLE, NOT_NULLABLE));

  std::vector<std::pair<uint32_t, const TupleSchema>> partitioned_schema;
  partitioned_schema.emplace_back(schema_a_family, schema_a);
  partitioned_schema.emplace_back(schema_b_family, schema_b);

  FailureOrOwned<Page> page_result =
      CreatePartitionedSchemaPage(partitioned_schema,
                                  HeapBufferAllocator::Get());
  ASSERT_TRUE(page_result.is_success());
  std::unique_ptr<Page> page(page_result.release());

  FailureOrOwned<std::vector<std::pair<uint32_t, const TupleSchema>>>
      read_schema_result = ReadPartitionedSchemaPage(*page);
  ASSERT_TRUE(read_schema_result.is_success());

  ASSERT_EQ(partitioned_schema.size(), read_schema_result->size());
  for (int index = 0; index < partitioned_schema.size(); index++) {
    ASSERT_EQ(partitioned_schema[index].first,
              (*read_schema_result)[index].first);
    bool equal_schema =
        TupleSchema::AreEqual(partitioned_schema[index].second,
                              (*read_schema_result)[index].second,
                              true /* check names */);
    ASSERT_TRUE(equal_schema);
  }
}

}  // namespace
}  // namespace supersonic
