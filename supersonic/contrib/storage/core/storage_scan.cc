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

#include "supersonic/contrib/storage/core/storage_scan.h"

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/text_format.h>
#include <supersonic/contrib/storage/core/page_reader.h>

#include <memory>
#include <string>
#include <vector>

#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/contrib/storage/base/storage.h"
#include "supersonic/contrib/storage/util/schema_converter.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/core/coalesce.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"


namespace supersonic {

namespace {

// TODO(wzoltak): Move somewhere else.
const std::string kDataStreamExtension = ".data";
const std::string kSchemaStreamName = "schema.meta";
const int kMaxRowCount = 8192;

// TODO(wzoltak): Comment.
class StorageScanCursor : public BasicCursor {
 public:
  StorageScanCursor(const TupleSchema& schema,
                    std::unique_ptr<Cursor> data_to_join)
      : BasicCursor(schema), data_to_join_(std::move(data_to_join)) {}

  virtual ResultView Next(rowcount_t max_row_count) {
    return data_to_join_->Next(max_row_count);
  }

  virtual bool IsWaitingOnBarrierSupported() const {
    return data_to_join_->IsWaitingOnBarrierSupported();
  }

  virtual CursorId GetCursorId() const { return STORAGE_SCAN; }

 private:
  std::unique_ptr<Cursor> data_to_join_;
};

}  // namespace

// TODO(wzoltak): comment
FailureOr<TupleSchema> ReadSchema(
    ReadableStorage* storage,
    BufferAllocator* buffer_allocator) {
  FailureOrOwned<ByteStreamReader> schema_stream_result =
      storage->CreateByteStreamReader(kSchemaStreamName);
  PROPAGATE_ON_FAILURE(schema_stream_result);
  std::unique_ptr<ByteStreamReader>
      schema_stream(schema_stream_result.release());

  const size_t kInitialSchemaBlockSize = 4096;
  std::unique_ptr<Buffer> buffer(
      buffer_allocator->Allocate(kInitialSchemaBlockSize));
  if (!buffer.get()) {
    THROW(new Exception(ERROR_MEMORY_EXCEEDED,
                        "Couldn't allocate block for storage schema."));
  }

  size_t buffer_capacity(kInitialSchemaBlockSize);
  size_t bytes_read = 0;
  int64_t read_last_time;
  do {
    uint8_t* destination = static_cast<uint8_t*>(buffer->data()) + bytes_read;
    FailureOr<int64_t> bytes_read_result =
        schema_stream->ReadBytes(destination, buffer_capacity - bytes_read);
    PROPAGATE_ON_FAILURE(bytes_read_result);
    read_last_time = bytes_read_result.get();

    bytes_read += read_last_time;
    if (buffer_capacity - bytes_read == 0) {
      buffer_capacity *= 2;
      if (buffer_allocator->Reallocate(buffer_capacity, buffer.get())) {
        THROW(new Exception(ERROR_MEMORY_EXCEEDED,
                            "Couldn't allocate block for storage schema."));
      }
    }
  } while (read_last_time > 0);

  SchemaProto schema_proto;
  ::google::protobuf::io::ArrayInputStream
      array_input_stream(buffer->data(), bytes_read);
  ::google::protobuf::TextFormat::Parse(&array_input_stream, &schema_proto);

  FailureOr<TupleSchema> tuple_schema_result =
     SchemaConverter::SchemaProtoToTupleSchema(schema_proto);
  PROPAGATE_ON_FAILURE(tuple_schema_result);

  PROPAGATE_ON_FAILURE(schema_stream->Finalize());
  return Success(tuple_schema_result.get());
}

FailureOrOwned<Cursor> StorageScan(std::unique_ptr<ReadableStorage> storage,
                                   BufferAllocator* allocator) {
  FailureOr<TupleSchema> schema_result = ReadSchema(storage.get(), allocator);
  PROPAGATE_ON_FAILURE(schema_result);
  TupleSchema schema = schema_result.get();

  // Create PositionalJoin with PageReadersCursors as input.
  std::vector<Cursor*> inputs;
  for (size_t index = 0; index < schema.attribute_count(); index++) {
    Attribute attribute = schema.attribute(index);

    TupleSchema page_reader_schema;
    page_reader_schema.add_attribute(attribute);

    FailureOrOwned<PageStreamReader> page_stream_result =
        storage->CreatePageStreamReader(
            attribute.name() + kDataStreamExtension);
    PROPAGATE_ON_FAILURE(page_stream_result);
    std::unique_ptr<PageStreamReader> page_stream(page_stream_result.release());

    FailureOrOwned<Cursor> page_reader = PageReader(page_reader_schema,
                                                    std::move(page_stream),
                                                    allocator);
    PROPAGATE_ON_FAILURE(page_reader);
    inputs.push_back(page_reader.release());
  }

  FailureOrOwned<Cursor> coalesce_result = BoundCoalesce(inputs);
  PROPAGATE_ON_FAILURE(coalesce_result);
  std::unique_ptr<Cursor> coalesce(coalesce_result.release());

  return Success(new StorageScanCursor(schema, std::move(coalesce)));
}

}  // namespace supersonic
