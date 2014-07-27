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

#include "supersonic/contrib/storage/core/storage_sink.h"

#include <google/protobuf/text_format.h>
#include <memory>
#include <string>
#include <vector>
#include <utility>

#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/contrib/storage/base/byte_stream_writer.h"
#include "supersonic/contrib/storage/base/page_stream_writer.h"
#include "supersonic/contrib/storage/base/raw_storage.h"
#include "supersonic/contrib/storage/base/schema_partitioner.h"
#include "supersonic/contrib/storage/base/storage_metadata.h"
#include "supersonic/contrib/storage/core/page_builder.h"
#include "supersonic/contrib/storage/core/page_sink.h"
#include "supersonic/contrib/storage/util/schema_converter.h"
#include "supersonic/contrib/storage/util/path_util.h"
#include "supersonic/utils/exception/failureor.h"

#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {
namespace {

typedef std::pair<uint32_t, const TupleSchema> Family;

// TODO(wzoltak): Move somewhere else. It is also required during reading.
const uint32_t kMetadataPageFamily = 0;
const uint32_t kFirstDataPageFamily = 1;

const uint32_t kMaxDataBytesInFile = 128 * 1024 * 1024; // 128MB

// Represents a Sink which can write data into persistent storage.
// Splits data into single-attribute views and writes them into separate
// PageSink objects.
class SingleFileStorageSink : public Sink {
 public:
  typedef vector<std::unique_ptr<PageSink>> PageSinkVector;
  typedef vector<std::unique_ptr<const SingleSourceProjector>>
      SingleSourceProjectorVector;

  SingleFileStorageSink(std::unique_ptr<PageSinkVector> page_sinks,
              std::unique_ptr<SingleSourceProjectorVector> projectors,
              std::shared_ptr<PageStreamWriter> page_stream_writer,
              std::shared_ptr<MetadataWriter> metadata_writer)
      : page_sinks_(std::move(page_sinks)),
        projectors_(std::move(projectors)),
        page_stream_writer_(page_stream_writer),
        metadata_writer_(metadata_writer),
        finalized_(false) {}

  virtual ~SingleFileStorageSink() {
    if (!finalized_) {
      LOG(DFATAL) << "Destroying not finalized StorageSink.";
      Finalize();
    }
  }

  virtual FailureOr<rowcount_t> Write(const View& data) {
    if (finalized_) {
      THROW(new Exception(ERROR_INVALID_STATE,
                          "Writing to finalized StorageSink."));
    }

    const rowcount_t row_count = data.row_count();
    for (std::unique_ptr<PageSink>& page_sink : *page_sinks_) {
      FailureOr<rowcount_t> result = page_sink->Write(data);
      PROPAGATE_ON_FAILURE(result);

      // TODO(wzoltak): Handle incomplete writes?
      if (result.get() != row_count) {
        THROW(new Exception(ERROR_GENERAL_IO_ERROR,
                            "Inconsistent write to PageSink."));
      }
    }
    return Success(row_count);
  }

  virtual FailureOrVoid Finalize() {
    for (std::unique_ptr<PageSink>& page_sink : *page_sinks_) {
      FailureOrVoid result = page_sink->Finalize();
      PROPAGATE_ON_FAILURE(result);
    }
    PROPAGATE_ON_FAILURE(WriteMetadata());
    PROPAGATE_ON_FAILURE(page_stream_writer_->Finalize());
    finalized_ = true;
    return Success();
  }

  size_t WrittenBytes() {
    size_t result = page_stream_writer_->WrittenBytes();
    for (std::unique_ptr<PageSink>& page_sink : *page_sinks_) {
      result += page_sink->BytesInPage();
    }
    return result;
  }

 private:
  FailureOrVoid WriteMetadata() {
    auto metadata_page_result = metadata_writer_->DumpToPage();
    PROPAGATE_ON_FAILURE(metadata_page_result);
    PROPAGATE_ON_FAILURE(
        page_stream_writer_->AppendPage(kMetadataPageFamily,
                                        *metadata_page_result));
    return Success();
  }

  std::unique_ptr<PageSinkVector> page_sinks_;
  std::unique_ptr<SingleSourceProjectorVector> projectors_;
  std::shared_ptr<PageStreamWriter> page_stream_writer_;
  std::shared_ptr<MetadataWriter> metadata_writer_;
  bool finalized_;
  DISALLOW_COPY_AND_ASSIGN(SingleFileStorageSink);
};


// Enumerates schema partitions starting from given number.
std::unique_ptr<std::vector<Family>>
    EnumeratePartitions(const std::vector<TupleSchema>& partitions, int start) {
  std::unique_ptr<std::vector<Family>> enumerated(new std::vector<Family>());
  for (const TupleSchema& partition : partitions) {
    enumerated->emplace_back(start, partition);
    start++;
  }
  return std::move(enumerated);
}


// Creates a SingleSourceProjector for single family.
std::unique_ptr<const SingleSourceProjector>
    CreateFamilyProjector(Family family) {
  std::vector<std::string> attributes_names;
  for (int index = 0; index < family.second.attribute_count(); index++) {
    const Attribute& attribute = family.second.attribute(index);
    attributes_names.emplace_back(attribute.name());
  }
  return std::unique_ptr<const SingleSourceProjector>(
      ProjectNamedAttributes(attributes_names));
}


FailureOrOwned<std::vector<std::unique_ptr<PageSink>>>
    CreatePageSinks(const std::vector<Family>& families,
                    TupleSchema schema,
                    std::shared_ptr<PageStreamWriter> page_stream,
                    std::shared_ptr<MetadataWriter> metadata_writer,
                    BufferAllocator* allocator) {
  std::unique_ptr<std::vector<std::unique_ptr<PageSink>>>
      page_sinks(new std::vector<std::unique_ptr<PageSink>>);

  for (const Family& family : families) {
    std::unique_ptr<const SingleSourceProjector> projector =
        CreateFamilyProjector(family);
    FailureOrOwned<const BoundSingleSourceProjector> bound_projector_result =
        projector->Bind(schema);
    PROPAGATE_ON_FAILURE(bound_projector_result);
    std::unique_ptr<const BoundSingleSourceProjector>
        bound_projector(bound_projector_result.release());

    FailureOrOwned<PageSink> page_sink =
          CreatePageSink(std::move(bound_projector),
                         page_stream,
                         metadata_writer,
                         family.first,
                         allocator);
    PROPAGATE_ON_FAILURE(page_sink);
    page_sinks->emplace_back(page_sink.release());
  }

  return Success(page_sinks.release());
}


// Sink which writes to sequence of SingleFileStorageSinks. Next sink is
// requested when size of current exceeds the limit. Final size of single
// sink depends on writes contents and is slightly bigger then the limit.
// Also, at this point size calculation does not include storage metadata
// and raw storage index.
class StorageSink : public Sink {
 public:
  StorageSink(const TupleSchema& schema,
              std::unique_ptr<WritableRawStorage> storage,
              BufferAllocator* allocator)
      : schema_(schema),
        storage_(std::move(storage)),
        allocator_(allocator),
        finalized_(false) {}

  virtual ~StorageSink() {}

  FailureOr<rowcount_t> Write(const View& data) {
    PROPAGATE_ON_FAILURE(MaybeChangeSink());
    return storage_sink_->Write(data);
  }

  FailureOrVoid Finalize() {
    PROPAGATE_ON_FAILURE(storage_sink_->Finalize());
    finalized_ = true;
    return Success();
  }

 private:
  FailureOrVoid MaybeChangeSink() {
    if (finalized_ || (storage_sink_.get() != nullptr &&
        storage_sink_->WrittenBytes() < kMaxDataBytesInFile)) {
      return Success();
    }

    std::unique_ptr<vector<std::unique_ptr<const SingleSourceProjector>>>
          projectors(new vector<std::unique_ptr<const SingleSourceProjector>>());

    // Create page stream
    FailureOrOwned<PageStreamWriter> page_stream_result =
        storage_->NextPageStreamWriter();
    PROPAGATE_ON_FAILURE(page_stream_result);
    page_stream_.reset(page_stream_result.release());

    // Partition schema
    // TODO(wzoltak): magic constant
    FailureOrOwned<SchemaPartitioner> partitioner_result =
        CreateFixedSizeSchemaParitioner(2);
    PROPAGATE_ON_FAILURE(partitioner_result);
    std::unique_ptr<SchemaPartitioner> partitioner(partitioner_result.release());

    FailureOrOwned<std::vector<TupleSchema>> partitions =
        partitioner->Partition(schema_);
    PROPAGATE_ON_FAILURE(partitions);
    std::unique_ptr<std::vector<Family>> families
        = EnumeratePartitions(*partitions, kFirstDataPageFamily);

    // Create MetadataWriter
    FailureOrOwned<MetadataWriter> metadata_writer_result =
        CreateMetadataWriter(*families, allocator_);
    PROPAGATE_ON_FAILURE(metadata_writer_result);
    std::shared_ptr<MetadataWriter>
        metadata_writer(metadata_writer_result.release());

    // Create PageSinks
    FailureOrOwned<vector<std::unique_ptr<PageSink>>> page_sinks_result =
        CreatePageSinks(*families,
                        schema_,
                        page_stream_,
                        metadata_writer,
                        allocator_);
    PROPAGATE_ON_FAILURE(page_sinks_result);
    std::unique_ptr<std::vector<std::unique_ptr<PageSink>>>
        page_sinks(page_sinks_result.release());

    if (storage_sink_.get() != nullptr) {
      PROPAGATE_ON_FAILURE(storage_sink_->Finalize());
    }

    storage_sink_.reset(new SingleFileStorageSink(std::move(page_sinks),
                                                  std::move(projectors),
                                                  page_stream_,
                                                  metadata_writer));
    return Success();
  }

  const TupleSchema schema_;
  std::unique_ptr<SingleFileStorageSink> storage_sink_;
  std::shared_ptr<PageStreamWriter> page_stream_;
  std::unique_ptr<WritableRawStorage> storage_;
  BufferAllocator* allocator_;
  bool finalized_;
};


}  // namespace


FailureOrOwned<Sink> CreateMultiFilesStorageSink(
    const TupleSchema& schema,
    std::unique_ptr<WritableRawStorage> storage,
    BufferAllocator* allocator) {
  return Success(new StorageSink(schema,
                                 std::move(storage),
                                 allocator));
}


FailureOrOwned<Sink> CreateFileStorageSink(
    const TupleSchema& schema,
    std::unique_ptr<WritableRawStorage> storage,
    BufferAllocator* allocator) {
  std::unique_ptr<vector<std::unique_ptr<const SingleSourceProjector>>>
      projectors(new vector<std::unique_ptr<const SingleSourceProjector>>());

  // Create page stream
  FailureOrOwned<PageStreamWriter> page_stream_result =
      storage->NextPageStreamWriter();
  PROPAGATE_ON_FAILURE(page_stream_result);
  std::shared_ptr<PageStreamWriter> page_stream(page_stream_result.release());

  // Partition schema
  // TODO(wzoltak): magic constant
  FailureOrOwned<SchemaPartitioner> partitioner_result =
      CreateFixedSizeSchemaParitioner(2);
  PROPAGATE_ON_FAILURE(partitioner_result);
  std::unique_ptr<SchemaPartitioner> partitioner(partitioner_result.release());

  FailureOrOwned<std::vector<TupleSchema>> partitions =
      partitioner->Partition(schema);
  PROPAGATE_ON_FAILURE(partitions);
  std::unique_ptr<std::vector<Family>> families
      = EnumeratePartitions(*partitions, kFirstDataPageFamily);

  // Create MetadataWriter
  FailureOrOwned<MetadataWriter> metadata_writer_result =
      CreateMetadataWriter(*families, allocator);
  PROPAGATE_ON_FAILURE(metadata_writer_result);
  std::shared_ptr<MetadataWriter>
      metadata_writer(metadata_writer_result.release());

  // Create PageSinks
  FailureOrOwned<vector<std::unique_ptr<PageSink>>> page_sinks_result =
      CreatePageSinks(*families,
                      schema,
                      page_stream,
                      metadata_writer,
                      allocator);
  PROPAGATE_ON_FAILURE(page_sinks_result);
  std::unique_ptr<std::vector<std::unique_ptr<PageSink>>>
      page_sinks(page_sinks_result.release());

  return Success(new SingleFileStorageSink(std::move(page_sinks),
                                           std::move(projectors),
                                           page_stream,
                                           metadata_writer));
}


// For testing purposes only.
FailureOrOwned<Sink> CreateStorageSink(
    std::unique_ptr<std::vector<std::unique_ptr<PageSink>>> page_sinks,
    std::shared_ptr<PageStreamWriter> page_stream,
    std::shared_ptr<MetadataWriter> metadata_writer) {
  std::unique_ptr<std::vector<
      std::unique_ptr<const SingleSourceProjector>>> projectors(
          new std::vector<std::unique_ptr<const SingleSourceProjector>>());
  return Success(new SingleFileStorageSink(std::move(page_sinks),
                                           std::move(projectors),
                                           page_stream,
                                           metadata_writer));
}

}  // namespace supersonic
