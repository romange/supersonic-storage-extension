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

#ifndef SUPERSONIC_CONTRIB_STORAGE_BASE_COLUMN_WRITER_H_
#define SUPERSONIC_CONTRIB_STORAGE_BASE_COLUMN_WRITER_H_

#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"

#include "supersonic/contrib/storage/core/page_builder.h"

namespace supersonic {

// Writes data from column into PageBuilder. May use more than one stream
// (e.g. in order to handle nullability).
class ColumnWriter {
 public:
  virtual ~ColumnWriter() {}

  // Writes Column into given page_builder.
  virtual FailureOrVoid WriteColumn(const Column& column,
                                    rowcount_t row_count) = 0;

  // Returns a number of consecutive streams used during write.
  int uses_streams() { return uses_streams_; }

 protected:
  explicit ColumnWriter(int uses_streams) : uses_streams_(uses_streams) {}
  int uses_streams_;

 private:
  DISALLOW_COPY_AND_ASSIGN(ColumnWriter);
};

// Creates a ColumnWriter writing Column objects into given PageBuilder,
// using consecutive byte buffers starting from `first_byte_buffer`.
FailureOrOwned<ColumnWriter> CreateColumnWriter(
    const Attribute& attribute,
    std::shared_ptr<PageBuilder> page_builder,
    int first_byte_buffer);

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_BASE_COLUMN_WRITER_H_
