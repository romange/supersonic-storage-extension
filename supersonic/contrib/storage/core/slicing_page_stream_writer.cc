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

#include "supersonic/contrib/storage/core/slicing_page_stream_writer.h"

#include <google/protobuf/text_format.h>
#include <glog/logging.h>

#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/contrib/storage/base/storage.h"
#include "supersonic/contrib/storage/core/schema_serialization.h"
#include "supersonic/contrib/storage/util/schema_converter.h"


namespace supersonic {
namespace {

const size_t kMaxFileSize = 1024 * 1024;  // 1MB

class SlicingPageStreamWriter : public PageStreamWriter {
 public:
  SlicingPageStreamWriter(std::unique_ptr<Page> schema_page,
                          std::unique_ptr<WritableStorage> storage)
     : schema_page_(std::move(schema_page)),
       storage_(std::move(storage)),
       written_(0),
       finalized_(false) {}

  virtual ~SlicingPageStreamWriter() {
    if (!finalized_) {
      LOG(DFATAL) << "Destroying not finalized SlicingPageStream.";
      Finalize();
    }
  }

  FailureOrVoid AppendPage(const Page& page) {
    if (finalized_) {
      THROW(new Exception(
          ERROR_INVALID_STATE,
          "Writing to finalized SlicingPageStream."));
    }
    PROPAGATE_ON_FAILURE(MaybeOpenNewStream(page));
    written_ += page.PageHeader().total_size;
    return page_stream_->AppendPage(page);
  }

  FailureOrVoid Finalize() {
    PROPAGATE_ON_FAILURE(page_stream_->Finalize());
    finalized_ = true;
    return Success();
  }

 private:
  FailureOrVoid MaybeOpenNewStream(const Page& page) {
    const bool no_page_stream = page_stream_.get() == nullptr;
    const bool empty_page = written_ == 0;
    const bool too_much_data =
        written_ + page.PageHeader().total_size > kMaxFileSize;
    if (no_page_stream || (!empty_page && too_much_data)) {
      if (page_stream_.get() != nullptr) {
        PROPAGATE_ON_FAILURE(page_stream_->Finalize());
      }

      FailureOrOwned<PageStreamWriter> page_stream_result =
          storage_->NextPageStreamWriter();
      PROPAGATE_ON_FAILURE(page_stream_result);
      page_stream_.reset(page_stream_result.release());

      written_ = 0;

      return AppendPage(*schema_page_);
    }

    return Success();
  }

  std::unique_ptr<Page> schema_page_;
  std::unique_ptr<WritableStorage> storage_;
  std::unique_ptr<PageStreamWriter> page_stream_;
  size_t written_;
  bool finalized_;
  DISALLOW_COPY_AND_ASSIGN(SlicingPageStreamWriter);
};

}  // namespace


FailureOrOwned<PageStreamWriter> CreateSlicingPageStreamWriter(
    TupleSchema schema,
    std::unique_ptr<WritableStorage> storage,
    BufferAllocator* allocator) {
  FailureOrOwned<Page> schema_page_result =
      CreateSchemaPage(schema, allocator);
  PROPAGATE_ON_FAILURE(schema_page_result);
  std::unique_ptr<Page> schema_page(schema_page_result.release());

  return Success(new SlicingPageStreamWriter(std::move(schema_page),
                                             std::move(storage)));
}

}  // namespace supersonic
