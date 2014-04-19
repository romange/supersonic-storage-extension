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

#ifndef SUPERSONIC_CONTRIB_STORAGE_CORE_STORAGE_SCAN_H_
#define SUPERSONIC_CONTRIB_STORAGE_CORE_STORAGE_SCAN_H_

#include <memory>

#include "supersonic/base/exception/result.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/contrib/storage/base/storage.h"

namespace supersonic {

// Creates a Cursor which reads data from given storage. Takes ownership over
// storage.
FailureOrOwned<Cursor> StorageScan(std::unique_ptr<ReadableStorage> storage,
                                   BufferAllocator* allocator);


}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_CORE_STORAGE_SCAN_H_