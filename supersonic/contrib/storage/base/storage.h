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
// Storage is represented by series of page readers / writers. Those can be used
// to split storage into logical chunks. E.g. in case of file storage - each
// of them may represent a single file. Each of those "chunks" should be usable
// as a storage on its own.

#ifndef SUPERSONIC_CONTRIB_STORAGE_BASE_STORAGE_H_
#define SUPERSONIC_CONTRIB_STORAGE_BASE_STORAGE_H_

#include <string>

#include "supersonic/base/exception/result.h"
#include "supersonic/contrib/storage/base/page_stream_writer.h"
#include "supersonic/contrib/storage/base/random_page_reader.h"

namespace supersonic {

// Base interface for writable persistent storage.
class WritableStorage {
 public:
  virtual ~WritableStorage() {}

  // Returns next PageStreamWriter for given storage.
  virtual FailureOrOwned<PageStreamWriter> NextPageStreamWriter() = 0;
};

// Base interface for readable persistent storage.
class ReadableStorage {
 public:
  virtual ~ReadableStorage() {}

  // Returns next RandomPageReader for given storage.
  virtual FailureOrOwned<RandomPageReader> NextRandomPageReader() = 0;

  virtual bool HasNext() = 0;
};

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_BASE_STORAGE_H_
