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

#include "supersonic/contrib/storage/core/merging_page_stream_reader.h"

#include <glog/logging.h>
#include <google/protobuf/text_format.h>

#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/contrib/storage/base/storage.h"
#include "supersonic/contrib/storage/util/schema_converter.h"


namespace supersonic {
namespace {

class MergingPageStreamReader : public PageStreamReader {
 public:
  explicit MergingPageStreamReader(std::unique_ptr<ReadableStorage> storage)
      : storage_(std::move(storage)),
        finalized_(false) {
    ResetCounters();
  }

  virtual ~MergingPageStreamReader() {
    if (!finalized_) {
      LOG(DFATAL) << "Destroying not finalized MergingPageStream.";
      Finalize();
    }
  }

  // Initializes the instance. Must be called before any other method.
  FailureOrVoid Init() {
    return OpenNewStream();
  }

  FailureOr<const Page*> NextPage() {
    if (finalized_) {
      THROW(new Exception(
          ERROR_INVALID_STATE,
          "Writing to finalized MergingPageStream."));
    }

    if (next_page_ >= page_reader_->TotalPages()) {
      if (storage_->HasNext()) {
        OpenNewStream();
      } else {
        return Success(Page::EmptyPage());
      }
    }
    FailureOr<const Page*> page_result =
        page_reader_->GetPage(next_page_offset_);
    PROPAGATE_ON_FAILURE(page_result);

    next_page_offset_ += page_result.get()->PageHeader().total_size;
    next_page_++;
    return Success(page_result.get());
  }

  FailureOrVoid Finalize() {
    PROPAGATE_ON_FAILURE(page_reader_->Finalize());
    finalized_ = true;
    return Success();
  }

 private:
  void ResetCounters() {
    next_page_ = 0;
    next_page_offset_ = 0;
  }

  FailureOrVoid OpenNewStream() {
    bool initial_reader = page_reader_.get() == nullptr;
    if (!initial_reader) {
      page_reader_->Finalize();
    }
    FailureOrOwned<RandomPageReader> page_reader_result =
        storage_->NextRandomPageReader();
    PROPAGATE_ON_FAILURE(page_reader_result);
    page_reader_.reset(page_reader_result.release());
    ResetCounters();

    if (!initial_reader) {
      // First page contains schema. Discard it.
      PROPAGATE_ON_FAILURE(NextPage());
    }

    return Success();
  }

  std::unique_ptr<ReadableStorage> storage_;
  std::unique_ptr<RandomPageReader> page_reader_;
  uint64_t next_page_;
  uint64_t next_page_offset_;
  bool finalized_;
  DISALLOW_COPY_AND_ASSIGN(MergingPageStreamReader);
};

}  // namespace


FailureOrOwned<PageStreamReader> CreateMergingPageStreamReader(
    std::unique_ptr<ReadableStorage> storage) {
  std::unique_ptr<MergingPageStreamReader> merging_page_stream_reader(
      new MergingPageStreamReader(std::move(storage)));
  merging_page_stream_reader->Init();
  return Success(merging_page_stream_reader.release());
}

}  // namespace supersonic
