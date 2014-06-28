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

#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/contrib/storage/base/storage.h"
#include "supersonic/contrib/storage/base/byte_stream_writer.h"
#include "supersonic/contrib/storage/base/page_stream_writer.h"
#include "supersonic/contrib/storage/core/file_storage.h"
#include "supersonic/contrib/storage/core/page_builder.h"
#include "supersonic/contrib/storage/core/page_sink.h"
#include "supersonic/contrib/storage/core/slicing_page_stream_writer.h"
#include "supersonic/contrib/storage/util/schema_converter.h"
#include "supersonic/contrib/storage/util/path_util.h"
#include "supersonic/utils/exception/failureor.h"

#include "supersonic/proto/supersonic.pb.h"


namespace supersonic {

// TODO(wzoltak): Move somewhere else. It is required during reading.
const std::string kDataStreamExtension = ".data";
const std::string kSchemaStreamName = "schema.meta";

// Represents a Sink which can write data into persistent storage.
// Splits data into single-attribute views and writes them into separate
// PageSink objects.
class StorageSink : public Sink {
 public:
  typedef vector<std::unique_ptr<Sink> > PageSinkVector;
  typedef vector<std::unique_ptr<const SingleSourceProjector> >
      SingleSourceProjectorVector;

  StorageSink(
      std::unique_ptr<PageSinkVector> page_sinks,
      std::unique_ptr<SingleSourceProjectorVector> projectors)
      : page_sinks_(std::move(page_sinks)),
        projectors_(std::move(projectors)),
        finalized_(false) {}

  virtual ~StorageSink() {
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
    for (std::unique_ptr<Sink> &page_sink : *page_sinks_) {
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
    for (std::unique_ptr<Sink> &page_sink : *page_sinks_) {
      FailureOrVoid result = page_sink->Finalize();
      PROPAGATE_ON_FAILURE(result);
    }
    finalized_ = true;
    return Success();
  }

  // Returns a name of stream in which attribute will be stored.
  static std::string StreamName(const Attribute& attribute) {
    return attribute.name() + kDataStreamExtension;
  }

 private:
  std::unique_ptr<PageSinkVector> page_sinks_;
  std::unique_ptr<SingleSourceProjectorVector> projectors_;
  bool finalized_;
  DISALLOW_COPY_AND_ASSIGN(StorageSink);
};


FailureOrOwned<Sink> CreateFileStorageSink(
    const TupleSchema& schema,
    std::unique_ptr<WritableStorage> storage,
    BufferAllocator* buffer_allocator) {
  std::unique_ptr<vector<std::unique_ptr<Sink> > > page_sinks(
      new vector<std::unique_ptr<Sink> >());
  std::unique_ptr<vector<std::unique_ptr<const SingleSourceProjector> > >
      projectors(new vector<std::unique_ptr<const SingleSourceProjector> >());

  // Create page stream
  FailureOrOwned<PageStreamWriter> slicing_page_stream_result =
      CreateSlicingPageStreamWriter(schema,
                                    std::move(storage),
                                    buffer_allocator);
  PROPAGATE_ON_FAILURE(slicing_page_stream_result);
  std::unique_ptr<PageStreamWriter>
      slicing_page_stream(slicing_page_stream_result.release());

  // Create projector
  std::unique_ptr<const SingleSourceProjector>
      projector(ProjectAllAttributes());
  FailureOrOwned<const BoundSingleSourceProjector> bound_projector_result(
      projector->Bind(schema));
  PROPAGATE_ON_FAILURE(bound_projector_result);
  std::unique_ptr<const BoundSingleSourceProjector>
      bound_projector(bound_projector_result.release());

  // Create PageSink
  FailureOrOwned<Sink> page_sink =
      CreatePageSink(std::move(bound_projector),
                     std::move(slicing_page_stream),
                     buffer_allocator);
  PROPAGATE_ON_FAILURE(page_sink);

  page_sinks->emplace_back(page_sink.release());
  projectors->emplace_back(projector.release());

  std::unique_ptr<WritableStorage> empty_storage;

  return Success(new StorageSink(std::move(page_sinks),
                                 std::move(projectors)));
}

// For testing purposes only.
FailureOrOwned<Sink> CreateStorageSink(
    std::unique_ptr<std::vector<std::unique_ptr<Sink> > > page_sinks) {
  std::unique_ptr<std::vector<
      std::unique_ptr<const SingleSourceProjector> > > projectors(
          new std::vector<std::unique_ptr<const SingleSourceProjector> >());
  return Success(new StorageSink(std::move(page_sinks),
                                 std::move(projectors)));
}

}  // namespace supersonic
