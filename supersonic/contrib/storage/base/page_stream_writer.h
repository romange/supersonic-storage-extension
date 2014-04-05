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

#ifndef SUPERSONIC_CONTRIB_STORAGE_BASE_PAGE_STREAM_WRITER_H_
#define SUPERSONIC_CONTRIB_STORAGE_BASE_PAGE_STREAM_WRITER_H_

#include "supersonic/contrib/storage/base/page.h"

#include "supersonic/base/exception/result.h"

namespace supersonic {

class PageStreamWriter {
 public:
  virtual ~PageStreamWriter() {}

  virtual FailureOrVoid AppendPage(const Page& page) = 0;

  virtual FailureOrVoid Finalize() = 0;
};

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_BASE_PAGE_STREAM_WRITER_H_
