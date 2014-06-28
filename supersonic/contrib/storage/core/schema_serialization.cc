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

FailureOrOwned<Page> CreateSchemaPage(const TupleSchema& schema,
                                      BufferAllocator* allocator) {
  PageBuilder page_builder(1, allocator);

  std::string serialized_schema;
  FailureOrOwned<SchemaProto> schema_proto_result =
      SchemaConverter::TupleSchemaToSchemaProto(schema);
  PROPAGATE_ON_FAILURE(schema_proto_result);
  ::google::protobuf::TextFormat::PrintToString(*schema_proto_result,
                                                &serialized_schema);

  page_builder.AppendToByteBuffer(0,
                                  serialized_schema.c_str(),
                                  serialized_schema.length());

  return page_builder.CreatePage();
}


FailureOr<TupleSchema> ReadSchemaPage(const Page& page) {
  FailureOr<const ByteBufferHeader*> byte_buffer_header =
      page.ByteBufferHeader(0);
  PROPAGATE_ON_FAILURE(byte_buffer_header);

  FailureOr<const void*> byte_buffer = page.ByteBuffer(0);
  PROPAGATE_ON_FAILURE(byte_buffer);

  SchemaProto schema_proto;
  ::google::protobuf::io::ArrayInputStream
      array_input_stream(byte_buffer.get(),
                         byte_buffer_header.get()->length);
  ::google::protobuf::TextFormat::Parse(&array_input_stream, &schema_proto);

  FailureOr<TupleSchema> tuple_schema_result =
     SchemaConverter::SchemaProtoToTupleSchema(schema_proto);
  PROPAGATE_ON_FAILURE(tuple_schema_result);

  return Success(tuple_schema_result.get());
}

}  // namespace supersonic
