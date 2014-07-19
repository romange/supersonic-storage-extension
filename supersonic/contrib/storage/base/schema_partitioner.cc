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

#include <vector>

#include "supersonic/utils/exception/failureor.h"


namespace supersonic {
namespace {

class FixedSizeSchemaPartitioner : public SchemaPartitioner {
 public:
  explicit FixedSizeSchemaPartitioner(int partition_size)
      : partition_size_(partition_size) {}

  virtual ~FixedSizeSchemaPartitioner() {}

  FailureOrOwned<std::vector<TupleSchema>> Partition(TupleSchema schema) {
    std::unique_ptr<std::vector<TupleSchema>>
        partitions(new std::vector<TupleSchema>());
    int attribute_index = 0;
    int attribute_count = schema.attribute_count();
    do {
      TupleSchema partition;
      for (int i = 0;
          i < partition_size_ && attribute_index < attribute_count;
          i++, attribute_index++) {
        partition.add_attribute(schema.attribute(attribute_index));
      }
      partitions->push_back(partition);
    } while (attribute_index < attribute_count);

    return Success(partitions.release());
  }

 private:
  int partition_size_;
  DISALLOW_COPY_AND_ASSIGN(FixedSizeSchemaPartitioner);
};

}  // namespace

FailureOrOwned<SchemaPartitioner>
    CreateFixedSizeSchemaParitioner(int partition_size) {
  return Success(new FixedSizeSchemaPartitioner(partition_size));
}

}  // namespace supersonic
