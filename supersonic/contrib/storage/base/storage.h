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
//
// Contains basic interfaces for persistent storage. The storage is a stream,
// which can be either writable or readable, but not both at once.
// The storage provides access to named streams via reader / writer objects.

#ifndef SUPERSONIC_CONTRIB_STORAGE_BASE_STORAGE_H_
#define SUPERSONIC_CONTRIB_STORAGE_BASE_STORAGE_H_

#include <string>

#include "supersonic/base/exception/result.h"
#include "supersonic/contrib/storage/base/byte_stream_reader.h"
#include "supersonic/contrib/storage/base/byte_stream_writer.h"
#include "supersonic/contrib/storage/base/page_stream_reader.h"
#include "supersonic/contrib/storage/base/page_stream_writer.h"

namespace supersonic {

// Base interface for writable persistent storage.
class WritableStorage {
 public:
  virtual ~WritableStorage() {}

  // Creates a PageStreamWriter with a given name.
  virtual FailureOrOwned<PageStreamWriter> CreatePageStreamWriter(
      const std::string& name) = 0;

  // Creates a ByteStreamWriter with a given name.
  virtual FailureOrOwned<ByteStreamWriter> CreateByteStreamWriter(
      const std::string& name) = 0;
};

// Base interface for readable persistent storage.
class ReadableStorage {
 public:
  virtual ~ReadableStorage() {}

  // Creates a PageStreamReader for a given name.
  virtual FailureOrOwned<PageStreamReader> CreatePageStreamReader(
      const std::string& name) = 0;

  // Creates a ByteStreamReader for a given name.
  virtual FailureOrOwned<ByteStreamReader> CreateByteStreamReader(
      const std::string& name) = 0;
};

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_BASE_STORAGE_H_
