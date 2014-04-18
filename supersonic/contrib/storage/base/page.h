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

#ifndef SUPERSONIC_CONTRIB_STORAGE_BASE_PAGE_H_
#define SUPERSONIC_CONTRIB_STORAGE_BASE_PAGE_H_

#include <stdio.h>
#include <vector>

#include "supersonic/base/exception/result.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/memory/memory.h"

namespace supersonic {

// Contains basic page information.
struct PageHeader {
  uint64_t total_size;
  uint32_t byte_buffers_count;
};

// Contains basic byte buffor information.
struct ByteBufferHeader {
  uint64_t length;
  // TODO(wzoltak): compression method.
};

// Size of serialized PageHeader object in bytes.
const int kSerializedPageHeaderSize = 12;

// Size of serialized ByteBufferHeader object in bytes.
const int kSerializedByteBufferHeaderSize = 8;

// Size of serialized Page::Offset value in bytes.
const int kSerializedOffsetSize = 8;


// In-memory data chunk in storage layer. Contains a set of byte buffers.
// Exposes APIs that allow reading.
class Page {
 public:
  typedef uint64_t offset_t;

  virtual ~Page() {}

  // Returns a page header.
  virtual const struct PageHeader& PageHeader() const = 0;

  // Returns a header for given byte buffer.
  virtual FailureOr<const struct ByteBufferHeader*>
      ByteBufferHeader(int byte_buffer_index) const = 0;

  // Returns a pointer to place in which data of given byte buffer is stored.
  virtual FailureOr<const void*> ByteBuffer(int byte_buffer_index) const = 0;

  // Returns a pointer to raw in-memory page representation.
  virtual const void* RawData() const = 0;
};

// Creates Page object from buffer containing raw data. Takes ownership over
// the buffer.
FailureOrOwned<Page> CreatePage(std::unique_ptr<const Buffer> buffer);

// Creates Page object as a temporary view on buffer containing raw data. Does
// not take ownership over the buffer.
FailureOrOwned<Page> CreatePageView(const Buffer& buffer);

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_BASE_PAGE_H_
