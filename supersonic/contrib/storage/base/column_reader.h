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

#ifndef SUPERSONIC_CONTRIB_STORAGE_BASE_COLUMN_READER_H_
#define SUPERSONIC_CONTRIB_STORAGE_BASE_COLUMN_READER_H_

#include "supersonic/utils/macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/contrib/storage/base/page.h"

namespace supersonic {

// Class for reading single column from given page.
class ColumnReader {
 public:
  virtual ~ColumnReader() {}

  // Returns a view containing single column and rows read from given page.
  virtual FailureOr<const View*> ReadColumn(const Page& page) = 0;

  // Returns a number of consecutive byte buffers used during read.
  int uses_streams() { return uses_buffers_; }

 protected:
  explicit ColumnReader(int uses_buffers) : uses_buffers_(uses_buffers) {}
  int uses_buffers_;

 private:
  DISALLOW_COPY_AND_ASSIGN(ColumnReader);
};

// Creates a new ColumnReader. It will read a column described by given
// attribute using consecutive Page's byte buffers starting from given one.
FailureOrOwned<ColumnReader> CreateColumnReader(int first_buffer,
                                                const Attribute& attribute,
                                                BufferAllocator* allocator);

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_BASE_COLUMN_READER_H_
