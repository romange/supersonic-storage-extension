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

#ifndef SUPERSONIC_CONTRIB_STORAGE_CORE_PAGE_BUILDER_H_
#define SUPERSONIC_CONTRIB_STORAGE_CORE_PAGE_BUILDER_H_

#include <memory>

#include "supersonic/base/exception/result.h"
#include "supersonic/base/memory/arena.h"
#include "supersonic/contrib/storage/base/page.h"


namespace supersonic {

class PageBuilder {
 public:
  PageBuilder(unsigned int byte_buffers_count, BufferAllocator* allocator);
  ~PageBuilder();

  // Returns a number of byte buffers in builder.
  uint32_t ByteBuffersCount() const;

  // Returns a size in bytes of Page that would be created by CreatePage
  // in current builder state.
  uint64_t PageSize() const;

  // Appends `length` bytes from `data` into byte buffer number
  // `bute_buffer_index`.
  FailureOrVoid AppendToByteBuffer(unsigned int byte_buffer_index,
                                   const void* data,
                                   size_t length);

  // Appends `length` bytes into desired byte buffer and returns a pointer
  // to that memory chunk. Initial contents of buffer are not specified, but
  // one may use given pointer to overwrite contents in byte stream.
  FailureOr<void*> NextFromByteBuffer(
      unsigned int byte_buffer_index, size_t length);

  // Creates a Page containing accumulated data using newly allocated block.
  // Does not reset the builder.
  FailureOrOwned<Page> CreatePage();

  // Clears accumulated data. Preserves number of byte buffers.
  void Reset();

  // Clears accumulated data and changes number of byte buffers to
  // `byte_buffers_count`.
  void Reset(uint32_t byte_buffers_count);

 private:
  class Implementation;
  std::unique_ptr<Implementation> implementation_;

  DISALLOW_COPY_AND_ASSIGN(PageBuilder);
};


}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_CORE_PAGE_BUILDER_H_
