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

#include "supersonic/contrib/storage/base/schema_partitioner.h"

#include "gtest/gtest.h"

namespace supersonic {
namespace {

TEST(SchemaPartitionerTest, FixedSizeSchemaPartitioner) {
  TupleSchema schema;
  schema.add_attribute(Attribute("a", INT32, NULLABLE));
  schema.add_attribute(Attribute("b", INT32, NULLABLE));
  schema.add_attribute(Attribute("c", INT32, NULLABLE));
  schema.add_attribute(Attribute("d", INT32, NULLABLE));

  FailureOrOwned<SchemaPartitioner> partitioner_result =
      CreateFixedSizeSchemaParitioner(3);
  ASSERT_TRUE(partitioner_result.is_success());
  std::unique_ptr<SchemaPartitioner> partitioner(partitioner_result.release());

  FailureOrOwned<std::vector<TupleSchema>> partitions_result =
      partitioner->Partition(schema);
  ASSERT_TRUE(partitions_result.is_success());
  std::unique_ptr<std::vector<TupleSchema>>
      partitions(partitions_result.release());

  ASSERT_EQ(2, partitions->size());
  ASSERT_EQ(3, (*partitions)[0].attribute_count());
  ASSERT_EQ("b", (*partitions)[0].attribute(1).name());
  ASSERT_EQ(1, (*partitions)[1].attribute_count());
  ASSERT_EQ("d", (*partitions)[1].attribute(0).name());
}

}  // namespace
}  // namespace supersonic
