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
#include <utility>

#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/contrib/storage/base/storage.h"
#include "supersonic/contrib/storage/core/merging_page_stream_reader.h"
#include "supersonic/contrib/storage/core/page_reader.h"
#include "supersonic/contrib/storage/core/schema_serialization.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/core/coalesce.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"


namespace supersonic {
namespace {

typedef std::pair<uint32_t, const TupleSchema> Family;

// TODO(wzoltak): Move somewhere else.
const uint32_t kMetadataPageFamily = 0;

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


// For each given page family description creates a PageReader object.
FailureOrOwned<std::vector<Cursor*>>
    CreatePageReaders(const std::vector<Family>& partitioned_schema,
                      std::shared_ptr<RandomPageReader> page_reader,
                      BufferAllocator* allocator) {
  std::unique_ptr<std::vector<Cursor*>>
      page_readers(new std::vector<Cursor*>());

  for (const Family& family : partitioned_schema) {
    FailureOrOwned<Cursor> page_reader_result =
        PageReader(family.second,
                   page_reader,
                   family.first,
                   allocator);
    PROPAGATE_ON_FAILURE(page_reader_result);
    page_readers->push_back(page_reader_result.release());
  }

  return Success(page_readers.release());
}

}  // namespace


FailureOrOwned<Cursor>
    FileStorageScan(std::unique_ptr<ReadableStorage> storage,
                    BufferAllocator* allocator) {
  // Create PageStreamReader
  // Ownership will be shared between PageReaders.
  FailureOrOwned<RandomPageReader> random_page_reader_result =
      storage->NextRandomPageReader();
  PROPAGATE_ON_FAILURE(random_page_reader_result);
  std::shared_ptr<RandomPageReader>
      random_page_reader(random_page_reader_result.release());

  // Read schema
  FailureOr<const Page*> page_result =
      random_page_reader->GetPage(kMetadataPageFamily, 0 /* page number */);
  PROPAGATE_ON_FAILURE(page_result);
  FailureOrOwned<std::vector<std::pair<uint32_t, const TupleSchema>>>
      partitioned_schema = ReadPartitionedSchemaPage(*page_result.get());
  PROPAGATE_ON_FAILURE(partitioned_schema);

  // Create readers
  FailureOrOwned<std::vector<Cursor*>> page_readers_result =
      CreatePageReaders(*partitioned_schema, random_page_reader, allocator);
  PROPAGATE_ON_FAILURE(page_readers_result);

  FailureOrOwned<Cursor> coalesce_result = BoundCoalesce(*page_readers_result);
  PROPAGATE_ON_FAILURE(coalesce_result);
  std::unique_ptr<Cursor> coalesce(coalesce_result.release());

  TupleSchema coalesce_schema = coalesce->schema();

  return Success(
      new StorageScanCursor(coalesce_schema, std::move(coalesce)));
}

}  // namespace supersonic
