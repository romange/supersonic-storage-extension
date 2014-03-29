// Copyright 2014 Wojciech Żółtak. All Rights Reserved.
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
//

#include "supersonic/contrib/storage/core/page_sink.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/exception/exception.h"

namespace supersonic {

class PageSink : public Sink {
 public:
  explicit PageSink(std::unique_ptr<const BoundSingleSourceProjector> projector,
                    std::unique_ptr<PageStreamWriter> page_stream_writer)
      : finalized_(false),
        projector_(std::move(projector)),
        page_stream_writer_(std::move(page_stream_writer)) {}

  virtual ~PageSink() {}

  virtual FailureOr<rowcount_t> Write(const View& data) {
    THROW(new Exception(ERROR_NOT_IMPLEMENTED, "Not implemented."));
  }
  virtual FailureOrVoid Finalize() {
    // TODO(wzoltak): Implement;
    return Success();
  }

 private:
  bool finalized_;
  std::unique_ptr<const BoundSingleSourceProjector> projector_;
  std::unique_ptr<PageStreamWriter> page_stream_writer_;
  DISALLOW_COPY_AND_ASSIGN(PageSink);
};

FailureOrOwned<Sink> CreatePageSink(
    std::unique_ptr<const BoundSingleSourceProjector> projector,
    std::unique_ptr<PageStreamWriter> page_stream_writer,
    BufferAllocator* buffer_allocator) {
  std::unique_ptr<PageSink> sink(
      new PageSink(std::move(projector), std::move(page_stream_writer)));
  return Success(sink.release());
}


}  // namespace supersonic
