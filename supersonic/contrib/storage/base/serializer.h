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

#ifndef SUPERSONIC_CONTRIB_STORAGE_BASE_SERIALIZER_H_
#define SUPERSONIC_CONTRIB_STORAGE_BASE_SERIALIZER_H_

#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/contrib/storage/core/page_builder.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/utils/macros.h"

namespace supersonic {

// Base class for serializers, which are storing data into PageBuilders.
// Serializer may be stateful and adapt its behavior between calls.
class Serializer {
 public:
  Serializer() {};
  virtual ~Serializer() {};

  // Serializes given data, writing output into `output_buffer`. The support
  // for array in Supersonic is planned. Because of that this function
  // gets a multidimensional array `data`, described by `lengths` and `arrays`.
  //
  // TODO(wzoltak): When NULLs are represented as bitmasks it is necessary
  //                to user VariantConstPointer::raw(), since `bool_const_ptr`
  //                is not a CppType of any DataType.
  virtual FailureOrVoid Serialize(PageBuilder* output_buffer,
                                  int output_stream,
                                  VariantConstPointer data[],
                                  const size_t lengths[],
                                  const size_t arrays) = 0;
 private:
  DISALLOW_COPY_AND_ASSIGN(Serializer);
};

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_BASE_SERIALIZER_H_
