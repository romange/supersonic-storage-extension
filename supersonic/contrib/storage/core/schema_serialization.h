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

#ifndef SUPERSONIC_CONTRIB_STORAGE_CORE_SCHEMA_SERIALIZATION_H_
#define SUPERSONIC_CONTRIB_STORAGE_CORE_SCHEMA_SERIALIZATION_H_

#include <vector>

#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/contrib/storage/base/page.h"


namespace supersonic {

// Creates a page which contains a serialized list of (family, schema) pairs.
// The list is describing a partitioning of actual table schema. The data
// is serialized to PartitionedSchema protobuf message and dumped in binary
// form to byte buffer number 0.
FailureOrOwned<Page> CreatePartitionedSchemaPage(
    std::vector<std::pair<uint32_t, const TupleSchema>>& families,
    BufferAllocator* allocator);

// Reads partitioned schema from page, described in CreatePartitionedSchemaPage
// function.
FailureOrOwned<std::vector<std::pair<uint32_t, const TupleSchema>>>
    ReadPartitionedSchemaPage(const Page& page);

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_CORE_SCHEMA_SERIALIZATION_H_
