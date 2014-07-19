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

#ifndef SUPERSONIC_CONTRIB_STORAGE_BASE_SCHEMA_PARTITIONER_H_
#define SUPERSONIC_CONTRIB_STORAGE_BASE_SCHEMA_PARTITIONER_H_

#include <vector>

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/tuple_schema.h"


namespace supersonic {

// Basic interface for schema partitioners.
class SchemaPartitioner {
 public:
  virtual ~SchemaPartitioner() {}

  // Returns a vector of TupleSchema objects which represents a partition
  // of given schema.
  virtual FailureOrOwned<std::vector<TupleSchema>>
      Partition(TupleSchema schema) = 0;
};

// Creates a partitioner which creates partitions with given, fixed size
// (except the last one). Consecutive partitions have consistent subsequences
// of attributes from original schema (i.e. the order is maintained).
FailureOrOwned<SchemaPartitioner>
    CreateFixedSizeSchemaParitioner(int parition_size);

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_BASE_SCHEMA_PARTITIONER_H_
