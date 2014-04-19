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

#ifndef SUPERSONIC_CONTRIB_STORAGE_BASE_DESERIALIZER_H_
#define SUPERSONIC_CONTRIB_STORAGE_BASE_DESERIALIZER_H_

#include <utility>

#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/contrib/storage/core/page_builder.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/utils/macros.h"

namespace supersonic {

// Base class for deserializers, which are reading data from byte buffers.
class Deserializer {
 public:
  Deserializer() {}
  virtual ~Deserializer() {}

  // Deserializes data from given byte buffer. Returns a pointer to buffer
  // containing deserialized data and row count.
  virtual FailureOr<pair<VariantConstPointer, rowcount_t> >
      Deserialize(const void* byte_buffer,
                  const ByteBufferHeader& byte_buffer_header) = 0;
 private:
  DISALLOW_COPY_AND_ASSIGN(Deserializer);
};

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_BASE_DESERIALIZER_H_
