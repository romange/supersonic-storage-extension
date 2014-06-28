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

#include <memory>
#include <string>
#include <vector>

#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/contrib/storage/base/storage.h"
#include "supersonic/contrib/storage/core/merging_page_stream_reader.h"
#include "supersonic/contrib/storage/core/page_reader.h"
#include "supersonic/contrib/storage/core/schema_serialization.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"


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


FailureOrOwned<Cursor>
    FileStorageScan(std::unique_ptr<ReadableStorage> storage,
                          BufferAllocator* allocator) {
  // Create PageStreamReader
  FailureOrOwned<PageStreamReader> page_stream_reader_result
      = CreateMergingPageStreamReader(std::move(storage));
  PROPAGATE_ON_FAILURE(page_stream_reader_result);
    std::unique_ptr<PageStreamReader>
        page_stream_reader(page_stream_reader_result.release());

  // Read schema
  FailureOr<const Page*> page_result = page_stream_reader->NextPage();
  PROPAGATE_ON_FAILURE(page_result);
  FailureOr<TupleSchema> schema = ReadSchemaPage(*page_result.get());
  PROPAGATE_ON_FAILURE(schema);

  // Create PageReader
  FailureOrOwned<Cursor> page_reader_result =
      PageReader(schema.get(),
                 std::move(page_stream_reader),
                 allocator);
  PROPAGATE_ON_FAILURE(page_reader_result);
  std::unique_ptr<Cursor> page_reader(page_reader_result.release());

  return Success(new StorageScanCursor(schema.get(), std::move(page_reader)));
}

}  // namespace supersonic
