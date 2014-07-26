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
#include "supersonic/contrib/storage/base/storage_metadata.h"
#include "supersonic/contrib/storage/core/page_reader.h"
#include "supersonic/contrib/storage/util/finally.h"
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
                    std::unique_ptr<Cursor> data_to_join,
                    std::unique_ptr<const SingleSourceProjector> projector,
                    std::unique_ptr<const BoundSingleSourceProjector>
                    bound_projector)
      : BasicCursor(schema),
        data_to_join_(std::move(data_to_join)),
        projector_(std::move(projector)),
        bound_projector_(std::move(bound_projector)) {}

  virtual ResultView Next(rowcount_t max_row_count) {
    ResultView data = data_to_join_->Next(max_row_count);
    PROPAGATE_ON_FAILURE(data);

    if (data.has_data()) {
      bound_projector_->Project(data.view(), my_view());
      my_view()->set_row_count(data.view().row_count());
      return ResultView::Success(my_view());
    } else {
      return data;
    }
  }

  virtual bool IsWaitingOnBarrierSupported() const {
    return data_to_join_->IsWaitingOnBarrierSupported();
  }

  virtual CursorId GetCursorId() const { return STORAGE_SCAN; }

 private:
  std::unique_ptr<Cursor> data_to_join_;
  std::unique_ptr<const SingleSourceProjector> projector_;
  std::unique_ptr<const BoundSingleSourceProjector> bound_projector_;
};


class DataStorageImplementation : public DataStorage {
 public:
  DataStorageImplementation(std::shared_ptr<RandomPageReader> page_reader,
                            std::unique_ptr<StorageMetadata> metadata,
                            std::unique_ptr<TupleSchema> contents_schema,
                            BufferAllocator* allocator)
      : page_reader_(page_reader),
        metadata_(std::move(metadata)),
        contents_schema_(std::move(contents_schema)),
        allocator_(allocator) {}

  ~DataStorageImplementation() {
    // TODO(wzoltak): Not handled Failure!
//    page_reader_->Finalize();
  }

  const StorageMetadata& Metadata() const {
    return *metadata_;
  }

  const TupleSchema& ContentsSchema() const {
    return *contents_schema_;
  }

  FailureOrOwned<Cursor> CreateScanCursor(rowcount_t starting_from_row) {
    return CreateScanCursor(starting_from_row, ContentsSchema());
  }

  FailureOrOwned<Cursor> CreateScanCursor(rowcount_t starting_from_row,
                                          const TupleSchema& output_schema) {
    // Get output schema and set of required families.
    FailureOrOwned<std::set<uint32_t>> required_families =
        RequiredFamilies(output_schema, *metadata_);
    PROPAGATE_ON_FAILURE(required_families);

    // Create readers
    FailureOrOwned<std::vector<Cursor*>> page_readers_result =
        CreatePageReaders(*metadata_,
                          *required_families,
                          page_reader_,
                          starting_from_row,
                          allocator_);
    PROPAGATE_ON_FAILURE(page_readers_result);

    FailureOrOwned<Cursor> coalesce_result =
        BoundCoalesce(*page_readers_result);
    PROPAGATE_ON_FAILURE(coalesce_result);
    std::unique_ptr<Cursor> coalesce(coalesce_result.release());

    // Create projector
    std::unique_ptr<const SingleSourceProjector>
        projector(CreateProjector(output_schema));
    FailureOrOwned<const BoundSingleSourceProjector> bound_projector_result =
        projector->Bind(coalesce->schema());
    PROPAGATE_ON_FAILURE(bound_projector_result);
    std::unique_ptr<const BoundSingleSourceProjector>
        bound_projector(bound_projector_result.release());

    if (!TupleSchema::AreEqual(bound_projector->result_schema(),
                               output_schema,
                               true /* check names */)) {
      THROW(new Exception(ERROR_INVALID_ARGUMENT_VALUE,
                          "Types mismatch between schemas."));
    }

    TupleSchema coalesce_schema = coalesce->schema();
    return Success(new StorageScanCursor(output_schema,
                                         std::move(coalesce),
                                         std::move(projector),
                                         std::move(bound_projector)));
  }

 private:
  FailureOrOwned<std::set<uint32_t>>
      RequiredFamilies(const TupleSchema& schema,
                       const StorageMetadata& metadata) {
    std::unique_ptr<std::set<uint32_t>>
        required_families(new std::set<uint32_t>());

    std::map<std::string, uint32_t> attribute_map;
    for (auto& family : metadata.page_families()) {
      for (int index = 0; index < family.schema().attribute_size(); index++) {
        const AttributeProto& attribute = family.schema().attribute(index);
        attribute_map[attribute.name()] = family.family_number();
      }
    }

    for (int index = 0; index < schema.attribute_count(); index++) {
      const Attribute& attribute = schema.attribute(index);
      auto it = attribute_map.find(attribute.name());
      if (it == attribute_map.end()) {
        THROW(new Exception(
            ERROR_INVALID_ARGUMENT_VALUE,
            StringPrintf("Attribute '%s' not present in any family.",
                         attribute.name().c_str())));
      }

      required_families->insert(it->second);
    }
    return Success(required_families.release());
  }

  // For each given page family description creates a PageReader object.
  FailureOrOwned<std::vector<Cursor*>>
      CreatePageReaders(const StorageMetadata& storage_metadata,
                        const std::set<uint32_t>& required_families,
                        std::shared_ptr<RandomPageReader> page_reader,
                        rowcount_t starting_from_row,
                        BufferAllocator* allocator) {
    std::unique_ptr<std::vector<Cursor*>>
        page_readers(new std::vector<Cursor*>());

    for (const PageFamily& family : storage_metadata.page_families()) {
      auto it = required_families.find(family.family_number());
      if (it == required_families.end()) {
        continue;
      }

      FailureOrOwned<Cursor> page_reader_result =
          PageReader(page_reader,
                     family,
                     starting_from_row,
                     allocator);
      PROPAGATE_ON_FAILURE(page_reader_result);
      page_readers->push_back(page_reader_result.release());
    }

    return Success(page_readers.release());
  }

  const SingleSourceProjector* CreateProjector(const TupleSchema& schema) {
    std::vector<std::string> names;
    for (int index = 0; index < schema.attribute_count(); index++) {
      const Attribute& attribute = schema.attribute(index);
      names.push_back(attribute.name());
    }
    return ProjectNamedAttributes(names);
  }

  std::shared_ptr<RandomPageReader> page_reader_;
  std::unique_ptr<StorageMetadata> metadata_;
  std::unique_ptr<TupleSchema> contents_schema_;
  BufferAllocator* allocator_;
  DISALLOW_COPY_AND_ASSIGN(DataStorageImplementation);
};


class MultiFileStorageScan : public BasicCursor {
 public:
  MultiFileStorageScan(const TupleSchema& schema,
                       std::unique_ptr<DataStorage> data_storage,
                       std::unique_ptr<Cursor> initial_cursor,
                       std::unique_ptr<ReadableStorage> readable_storage,
                       BufferAllocator* allocator)
      : BasicCursor(schema),
        schema_(schema),
        data_storage_(std::move(data_storage)),
        cursor_(std::move(initial_cursor)),
        readable_storage_(std::move(readable_storage)),
        allocator_(allocator) {}


  ResultView Next(rowcount_t max_row_count) {
    ResultView data = cursor_->Next(max_row_count);
    PROPAGATE_ON_FAILURE(data);

    if (data.is_eos()) {
      printf("EOS!\n");
      if (!readable_storage_->HasNext()) {
        return data;
      }
      PROPAGATE_ON_FAILURE(NextDataStorageAndCursor());
      return Next(max_row_count);
    } else {
      return data;
    }
  }

  bool IsWaitingOnBarrierSupported() const {
    return cursor_->IsWaitingOnBarrierSupported();
  }

  CursorId GetCursorId() const { return STORAGE_SCAN; }

  FailureOrVoid InitialShift(rowcount_t row) {
    size_t data_storage_size = CurrentDataStorageSize();
    while (row >= data_storage_size) {
      PROPAGATE_ON_FAILURE(NextDataStorageAndCursor());
      row -= data_storage_size;
    }
    FailureOrOwned<Cursor> cursor_result =
        data_storage_->CreateScanCursor(row, schema_);
    PROPAGATE_ON_FAILURE(cursor_result);
    cursor_.reset(cursor_result.release());
    return Success();
  }

 private:
  size_t CurrentDataStorageSize() {
    const StorageMetadata& metadata = data_storage_->Metadata();
    size_t size = 0;
    CHECK_GT(metadata.page_families_size(), 0);
    const PageFamily& family = metadata.page_families(0);
    for (const PageMetadata& page_metadata : family.pages()) {
      size += page_metadata.row_count();
    }
    return size;
  }

  FailureOrVoid NextDataStorageAndCursor() {
    printf("[MultiFileStorageScan] Getting next cursor\n");
    FailureOrOwned<RandomPageReader> random_page_reader_result =
        readable_storage_->NextRandomPageReader();
    PROPAGATE_ON_FAILURE(random_page_reader_result);
    std::unique_ptr<RandomPageReader>
        random_page_reader(random_page_reader_result.release());


    FailureOrOwned<DataStorage> data_storage =
        CreateDataStorage(std::move(random_page_reader),
                          allocator_);
    PROPAGATE_ON_FAILURE(data_storage);
    FailureOrOwned<Cursor> cursor_result =
        data_storage->CreateScanCursor(0, schema_);
    PROPAGATE_ON_FAILURE(cursor_result);
    cursor_.reset(cursor_result.release());
    return Success();
  }

  TupleSchema schema_;
  std::unique_ptr<DataStorage> data_storage_;
  std::unique_ptr<Cursor> cursor_;
  std::unique_ptr<ReadableStorage> readable_storage_;
  BufferAllocator* allocator_;
};


FailureOrOwned<TupleSchema>
    ExtractSchema(const StorageMetadata& metadata) {
  std::unique_ptr<TupleSchema> schema(new TupleSchema());
  for (const PageFamily& family : metadata.page_families()) {
    FailureOr<TupleSchema> schema_chunk =
        SchemaConverter::SchemaProtoToTupleSchema(family.schema());
    PROPAGATE_ON_FAILURE(schema_chunk);
    for (int index = 0;
        index < schema_chunk.get().attribute_count();
        index++) {
      schema->add_attribute(schema_chunk.get().attribute(index));
    }
  }
  return Success(schema.release());
}

}  // namespace

FailureOrOwned<DataStorage>
    CreateDataStorage(std::unique_ptr<RandomPageReader> random_page_reader,
                      BufferAllocator* allocator) {
  // Read metadata
  FailureOr<const Page*> page_result =
      random_page_reader->GetPage(kMetadataPageFamily, 0);
  PROPAGATE_ON_FAILURE(page_result);
  FailureOrOwned<StorageMetadata> metadata_result =
      ReadStorageMetadata(*page_result.get());
  PROPAGATE_ON_FAILURE(metadata_result);
  std::unique_ptr<StorageMetadata> metadata(metadata_result.release());

  // Extract schema
  FailureOrOwned<TupleSchema> schema_result = ExtractSchema(*metadata);
  PROPAGATE_ON_FAILURE(schema_result);
  std::unique_ptr<TupleSchema> schema(schema_result.release());

  return Success(new DataStorageImplementation(std::move(random_page_reader),
                                               std::move(metadata),
                                               std::move(schema),
                                               allocator));
}


FailureOrOwned<Cursor>
    FileStorageScan(std::unique_ptr<ReadableStorage> storage,
                    rowcount_t starting_from_row,
                    BufferAllocator* allocator) {
  // Create PageStreamReader
  // Ownership will be shared between PageReaders.
  FailureOrOwned<RandomPageReader> random_page_reader_result =
      storage->NextRandomPageReader();
  PROPAGATE_ON_FAILURE(random_page_reader_result);
  std::unique_ptr<RandomPageReader>
      random_page_reader(random_page_reader_result.release());

  FailureOrOwned<DataStorage> data_storage =
      CreateDataStorage(std::move(random_page_reader), allocator);
  PROPAGATE_ON_FAILURE(data_storage);
  return data_storage->CreateScanCursor(starting_from_row);
}


FailureOrOwned<Cursor>
    FileStorageScan(std::unique_ptr<ReadableStorage> storage,
                    rowcount_t starting_from_row,
                    const TupleSchema& schema,
                    BufferAllocator* allocator) {
  // Create PageStreamReader
  // Ownership will be shared between PageReaders.
  FailureOrOwned<RandomPageReader> random_page_reader_result =
      storage->NextRandomPageReader();
  PROPAGATE_ON_FAILURE(random_page_reader_result);
  std::unique_ptr<RandomPageReader>
      random_page_reader(random_page_reader_result.release());

  FailureOrOwned<DataStorage> data_storage =
      CreateDataStorage(std::move(random_page_reader), allocator);
  PROPAGATE_ON_FAILURE(data_storage);
  return data_storage->CreateScanCursor(starting_from_row,
                                        schema);
}


FailureOrOwned<Cursor> MultiFilesScan(
    std::unique_ptr<ReadableStorage> storage,
    rowcount_t starting_from_row,
    const TupleSchema& schema,
    BufferAllocator* allocator) {
  // Create PageStreamReader
  // Ownership will be shared between PageReaders.
  FailureOrOwned<RandomPageReader> random_page_reader_result =
      storage->NextRandomPageReader();
  PROPAGATE_ON_FAILURE(random_page_reader_result);
  std::unique_ptr<RandomPageReader>
      random_page_reader(random_page_reader_result.release());

  FailureOrOwned<DataStorage> data_storage_result =
      CreateDataStorage(std::move(random_page_reader), allocator);
  PROPAGATE_ON_FAILURE(data_storage_result);
  std::unique_ptr<DataStorage> data_storage(data_storage_result.release());

  FailureOrOwned<Cursor> initial_cursor_result =
      data_storage->CreateScanCursor(0 /* starting_from_row */, schema);
  PROPAGATE_ON_FAILURE(initial_cursor_result);
  std::unique_ptr<Cursor> initial_cursor(initial_cursor_result.release());

  std::unique_ptr<MultiFileStorageScan>
      storage_scan(new MultiFileStorageScan(schema,
                                            std::move(data_storage),
                                            std::move(initial_cursor),
                                            std::move(storage),
                                            allocator));
  PROPAGATE_ON_FAILURE(storage_scan->InitialShift(starting_from_row));
  return Success(storage_scan.release());
}

}  // namespace supersonic
