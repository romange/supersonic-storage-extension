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
#include "supersonic/contrib/storage/core/page_builder.h"
#include "supersonic/contrib/storage/core/page_sink.h"
#include "supersonic/contrib/storage/util/schema_converter.h"
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
      std::unique_ptr<SingleSourceProjectorVector> projectors,
      std::unique_ptr<WritableStorage> storage)
      : page_sinks_(std::move(page_sinks)),
        projectors_(std::move(projectors)),
        storage_(std::move(storage)),
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
  std::unique_ptr<WritableStorage> storage_;
  bool finalized_;
  DISALLOW_COPY_AND_ASSIGN(StorageSink);
};

// Dumps TupleSchema into given storage in human-readable format.
// Serialization is done by conversion to SchemaProto and usage of
// google::protobuf::TextFormat.
FailureOrVoid DumpSchema(
    const TupleSchema& schema,
    WritableStorage* storage,
    BufferAllocator* buffer_allocator) {
  FailureOrOwned<ByteStreamWriter> schema_stream_result =
      storage->CreateByteStreamWriter(kSchemaStreamName);
  PROPAGATE_ON_FAILURE(schema_stream_result);
  std::unique_ptr<ByteStreamWriter>
      schema_stream(schema_stream_result.release());

  std::string serialized_schema;
  FailureOrOwned<SchemaProto> schema_proto =
      SchemaConverter::TupleSchemaToSchemaProto(schema);
  PROPAGATE_ON_FAILURE(schema_proto);
  ::google::protobuf::TextFormat::PrintToString(*schema_proto,
                                                &serialized_schema);

  FailureOrVoid dumped_schema = schema_stream->AppendBytes(
      serialized_schema.c_str(), serialized_schema.length());
  PROPAGATE_ON_FAILURE(dumped_schema);
  FailureOrVoid finalized_stream = schema_stream->Finalize();
  PROPAGATE_ON_FAILURE(finalized_stream);

  return Success();
}

FailureOrVoid DumpSchema(
    const TupleSchema& schema,
    PageStreamWriter* page_stream,
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

  FailureOrOwned<Page> page_result = page_builder.CreatePage();
  PROPAGATE_ON_FAILURE(page_result);
  std::unique_ptr<Page> page(page_result.release());
  PROPAGATE_ON_FAILURE(page_stream->AppendPage(*page));

  return Success();
}

FailureOrOwned<Sink> CreateStorageSink(
    const TupleSchema& schema,
    std::unique_ptr<WritableStorage> storage,
    BufferAllocator* buffer_allocator) {
  std::unique_ptr<vector<std::unique_ptr<Sink> > > page_sinks(
      new vector<std::unique_ptr<Sink> >());
  std::unique_ptr<vector<std::unique_ptr<const SingleSourceProjector> > >
      projectors(new vector<std::unique_ptr<const SingleSourceProjector> >());

  for (size_t i = 0; i < schema.attribute_count(); i++) {
    const Attribute& attribute = schema.attribute(i);
    std::unique_ptr<NamedAttributeProjector> projector(
        new NamedAttributeProjector(attribute.name()));

    FailureOrOwned<const BoundSingleSourceProjector> bound_projector_result =
        projector->Bind(schema);
    PROPAGATE_ON_FAILURE(bound_projector_result);
    std::unique_ptr<const BoundSingleSourceProjector>
        bound_projector(bound_projector_result.release());

    FailureOrOwned<PageStreamWriter> page_stream_result =
        storage->CreatePageStreamWriter(StorageSink::StreamName(attribute));
    PROPAGATE_ON_FAILURE(page_stream_result);
    std::unique_ptr<PageStreamWriter> page_stream(page_stream_result.release());

    FailureOrOwned<Sink> page_sink =
        CreatePageSink(std::move(bound_projector),
                       std::move(page_stream),
                       buffer_allocator);
    PROPAGATE_ON_FAILURE(page_sink);

    page_sinks->emplace_back(page_sink.release());
    projectors->emplace_back(projector.release());
  }

  FailureOrVoid schema_was_written =
      DumpSchema(schema, storage.get(), buffer_allocator);
  PROPAGATE_ON_FAILURE(schema_was_written);

  return Success(new StorageSink(std::move(page_sinks),
                                 std::move(projectors),
                                 std::move(storage)));
}


FailureOrOwned<Sink> CreateSingleFileStorageSink(
    const TupleSchema& schema,
    std::unique_ptr<WritableStorage> storage,
    BufferAllocator* buffer_allocator) {
  std::unique_ptr<vector<std::unique_ptr<Sink> > > page_sinks(
      new vector<std::unique_ptr<Sink> >());
  std::unique_ptr<vector<std::unique_ptr<const SingleSourceProjector> > >
      projectors(new vector<std::unique_ptr<const SingleSourceProjector> >());

  // Create page stream
  FailureOrOwned<PageStreamWriter> page_stream_result =
      storage->CreatePageStreamWriter("data");
  PROPAGATE_ON_FAILURE(page_stream_result);
  std::unique_ptr<PageStreamWriter> page_stream(page_stream_result.release());

  // Dump Schema
  DumpSchema(schema, page_stream.get(), buffer_allocator);

  // Create projector
  std::unique_ptr<const SingleSourceProjector> projector(ProjectAllAttributes());
  FailureOrOwned<const BoundSingleSourceProjector> bound_projector_result(
      projector->Bind(schema));
  PROPAGATE_ON_FAILURE(bound_projector_result);
  std::unique_ptr<const BoundSingleSourceProjector>
      bound_projector(bound_projector_result.release());

  // Create PageSink
  FailureOrOwned<Sink> page_sink =
      CreatePageSink(std::move(bound_projector),
                     std::move(page_stream),
                     buffer_allocator);
  PROPAGATE_ON_FAILURE(page_sink);

  page_sinks->emplace_back(page_sink.release());
  projectors->emplace_back(projector.release());

  return Success(new StorageSink(std::move(page_sinks),
                                 std::move(projectors),
                                 std::move(storage)));
}

// For testing purposes only.
FailureOrOwned<Sink> CreateStorageSink(
    std::unique_ptr<std::vector<std::unique_ptr<Sink> > > page_sinks,
    std::unique_ptr<WritableStorage> storage) {
  std::unique_ptr<std::vector<
      std::unique_ptr<const SingleSourceProjector> > > projectors(
          new std::vector<std::unique_ptr<const SingleSourceProjector> >());
  return Success(new StorageSink(std::move(page_sinks),
                                 std::move(projectors),
                                 std::move(storage)));
}

}  // namespace supersonic
