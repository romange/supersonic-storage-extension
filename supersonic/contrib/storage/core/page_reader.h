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

#ifndef SUPERSONIC_CONTRIB_STORAGE_CORE_PAGE_READER_H_
#define SUPERSONIC_CONTRIB_STORAGE_CORE_PAGE_READER_H_

#include <memory>

#include "supersonic/base/exception/result.h"
#include "supersonic/contrib/storage/base/random_page_reader.h"
#include "supersonic/cursor/base/cursor.h"


namespace supersonic {

// Creates a Cursor which reads data described by `schema` from given page
// stream.
FailureOrOwned<Cursor> PageReader(
    std::shared_ptr<RandomPageReader> page_reader,
    const PageFamily& page_family,
    rowcount_t starting_from_row,
    BufferAllocator* allocator);

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_CORE_PAGE_READER_H_
