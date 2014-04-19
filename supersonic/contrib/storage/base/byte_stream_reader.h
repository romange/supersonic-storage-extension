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

#ifndef SUPERSONIC_CONTRIB_STORAGE_BASE_BYTE_STREAM_READER_H_
#define SUPERSONIC_CONTRIB_STORAGE_BASE_BYTE_STREAM_READER_H_

#include "supersonic/base/exception/result.h"

namespace supersonic {

// Interface for reading from byte stream.
class ByteStreamReader {
 public:
  virtual ~ByteStreamReader() {}

  // Read max `size` bytes into buffer. Returns number of read bytes, 0 on
  // EOF. The only case in which returned number is lower than `size` is when
  // read data ends the file.
  virtual FailureOr<int64_t> ReadBytes(void* buffer, int64_t size) = 0;

  // Finalizes the ByteStreamReader. After the reader is finalized it can not
  // be used for reading anymore. Finalize must be always called before the
  // object is destroyed.
  virtual FailureOrVoid Finalize() = 0;
};

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_BASE_BYTE_STREAM_READER_H_
