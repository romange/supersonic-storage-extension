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

#include "supersonic/contrib/storage/core/page_sink.h"

#include <vector>

#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/contrib/storage/base/serializer.h"
#include "supersonic/contrib/storage/core/data_type_serializer.h"

#include "supersonic/utils/exception/failureor.h"

#include "supersonic/contrib/storage/core/page_builder.h"

#include "supersonic/contrib/storage/base/column_writer.h"

using common::Success;

namespace supersonic {

const uint64_t kPageSizeLimit = 100 * 1024;  // 100KB

class PageSink : public Sink {
 public:
  explicit PageSink(
      std::unique_ptr<const BoundSingleSourceProjector> projector,
      std::unique_ptr<PageStreamWriter> page_stream_writer,
      std::unique_ptr<
          std::vector<std::unique_ptr<ColumnWriter> > > column_writers,
      std::shared_ptr<PageBuilder> page_builder)
      : finalized_(false),
        builder_dirty_(false),
        projector_(std::move(projector)),
        page_stream_writer_(std::move(page_stream_writer)),
        column_writers_(std::move(column_writers)),
        page_builder_(page_builder) {
  }

  virtual ~PageSink() {
    if (!finalized_) {
      LOG(DFATAL)<< "Destroying not finalized PageSink.";
      Finalize();
    }
  }

  virtual FailureOr<rowcount_t> Write(const View& data) {
    if (finalized_) {
      THROW(new Exception(ERROR_INVALID_STATE,
              "Writing to finalized page sink."));
    }
    View projected_data(projector_->result_schema());
    projected_data.set_row_count(data.row_count());
    projector_->Project(data, &projected_data);

    // TODO(wzoltak): Check if schema matches in debug mode?

    for (int column_index = 0; column_index < projected_data.column_count();
        column_index++) {
      FailureOrVoid write_result = (*column_writers_)[column_index]
      ->WriteColumn(projected_data.column(column_index),
          projected_data.row_count());
      PROPAGATE_ON_FAILURE(write_result);
    }
    builder_dirty_ = builder_dirty_ || projected_data.row_count() > 0;

    if (page_builder_->PageSize() > kPageSizeLimit) {
      FailureOrVoid written_page = WritePage();
      PROPAGATE_ON_FAILURE(written_page);
    }
    return Success(projected_data.row_count());
  }

  virtual FailureOrVoid Finalize() {
    if (!finalized_) {
      if (builder_dirty_) {
        PROPAGATE_ON_FAILURE(WritePage());
      }
      page_stream_writer_->Finalize();
      finalized_ = true;
    }
    return Success();
  }

 private:
  FailureOrVoid WritePage() {
    FailureOrOwned<Page> page_result = page_builder_->CreatePage();
    PROPAGATE_ON_FAILURE(page_result);
    std::unique_ptr<Page> page(page_result.release());

    page_stream_writer_->AppendPage(*page);
    page_builder_->Reset();
    builder_dirty_ = false;

    return Success();
  }

  bool finalized_;
  bool builder_dirty_;
  std::unique_ptr<const BoundSingleSourceProjector> projector_;
  std::unique_ptr<PageStreamWriter> page_stream_writer_;
  std::unique_ptr<std::vector<std::unique_ptr<ColumnWriter> > > column_writers_;
  std::shared_ptr<PageBuilder> page_builder_;
  DISALLOW_COPY_AND_ASSIGN(PageSink);
};

FailureOrOwned<Sink> CreatePageSink(
    std::unique_ptr<const BoundSingleSourceProjector> projector,
    std::unique_ptr<PageStreamWriter> page_stream_writer,
    BufferAllocator* buffer_allocator) {
  std::unique_ptr<std::vector<std::unique_ptr<ColumnWriter> > > serializers(
      new std::vector<std::unique_ptr<ColumnWriter> >());
  std::shared_ptr<PageBuilder> page_builder(
      new PageBuilder(0, buffer_allocator));

  const TupleSchema& schema = projector->result_schema();
  int streams_count = 0;
  for (int i = 0; i < schema.attribute_count(); i++) {
    const Attribute& attribute = schema.attribute(i);
    FailureOrOwned<ColumnWriter> column_writer_result = CreateColumnWriter(
        attribute, page_builder, streams_count);
    PROPAGATE_ON_FAILURE(column_writer_result);
    streams_count += column_writer_result->uses_streams();
    serializers->emplace_back(column_writer_result.release());
  }

  page_builder->Reset(streams_count);

  std::unique_ptr<PageSink> sink(
      new PageSink(std::move(projector), std::move(page_stream_writer),
                   std::move(serializers), std::move(page_builder)));
  return Success(sink.release());
}

// Factory function for testing purposes.
FailureOrOwned<Sink> CreatePageSink(
    std::unique_ptr<const BoundSingleSourceProjector> projector,
    std::unique_ptr<PageStreamWriter> page_stream_writer,
    std::unique_ptr<
        std::vector<std::unique_ptr<ColumnWriter> > > column_writers,
    std::shared_ptr<PageBuilder> page_builder) {
  std::unique_ptr<PageSink> page_sink(
      new PageSink(std::move(projector), std::move(page_stream_writer),
                   std::move(column_writers), page_builder));
  return Success(page_sink.release());
}

}  // namespace supersonic
