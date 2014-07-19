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

#ifndef SUPERSONIC_CONTRIB_STORAGE_CORE_PAGE_SINK_H_
#define SUPERSONIC_CONTRIB_STORAGE_CORE_PAGE_SINK_H_

#include "supersonic/base/exception/result.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/contrib/storage/base/page_stream_writer.h"
#include "supersonic/cursor/infrastructure/writer.h"
#include "supersonic/utils/macros.h"


namespace supersonic {

// Creates a PageSink, which dumps projected data into given page stream.
// Takes ownership over projector.
FailureOrOwned<Sink> CreatePageSink(
    std::unique_ptr<const BoundSingleSourceProjector> projector,
    std::shared_ptr<PageStreamWriter> page_stream_writer,
    uint32_t page_family,
    BufferAllocator* buffer_allocator);

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_CORE_PAGE_SINK_H_
