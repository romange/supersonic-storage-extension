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

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/text_format.h>
#include <string>

#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/contrib/storage/core/page_builder.h"
#include "supersonic/contrib/storage/util/schema_converter.h"


namespace supersonic {

FailureOrOwned<Page> CreatePartitionedSchemaPage(
    std::vector<std::pair<uint32_t, const TupleSchema>>& families,
    BufferAllocator* allocator) {
  PageBuilder page_builder(1, allocator);

  PartitionedSchema partitioned_schema;
  for (std::pair<uint32_t, const TupleSchema>& family : families) {
    uint32_t family_number = family.first;
    FailureOrOwned<SchemaProto> schema =
        SchemaConverter::TupleSchemaToSchemaProto(family.second);
    PROPAGATE_ON_FAILURE(schema);

    AttributeFamily* attribute_family =
        partitioned_schema.add_attribute_family();
    attribute_family->set_family_number(family_number);
    attribute_family->set_allocated_schema(schema.release());
  }

  std::string serialized_families = partitioned_schema.SerializeAsString();
  page_builder.AppendToByteBuffer(0,
                                  serialized_families.c_str(),
                                  serialized_families.length());

  return page_builder.CreatePage();
}


FailureOrOwned<std::vector<std::pair<uint32_t, const TupleSchema>>>
    ReadPartitionedSchemaPage(const Page& page) {
  FailureOr<const ByteBufferHeader*> byte_buffer_header =
      page.ByteBufferHeader(0);
  PROPAGATE_ON_FAILURE(byte_buffer_header);

  FailureOr<const void*> byte_buffer = page.ByteBuffer(0);
  PROPAGATE_ON_FAILURE(byte_buffer);

  PartitionedSchema partitioned_schema;
  partitioned_schema.ParseFromArray(byte_buffer.get(),
                                    byte_buffer_header.get()->length);

  std::unique_ptr<std::vector<std::pair<uint32_t, const TupleSchema>>>
      schema(new std::vector<std::pair<uint32_t, const TupleSchema>>());
  for (int index = 0;
      index < partitioned_schema.attribute_family_size();
      index++) {
    const AttributeFamily& family = partitioned_schema.attribute_family(index);
    FailureOr<TupleSchema> family_schema =
        SchemaConverter::SchemaProtoToTupleSchema(family.schema());
    PROPAGATE_ON_FAILURE(family_schema);
    schema->emplace_back(family.family_number(), family_schema.get());
  }

  return Success(schema.release());
}

}  // namespace supersonic
