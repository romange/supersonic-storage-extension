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

#ifndef SUPERSONIC_CONTRIB_STORAGE_CORE_DATA_TYPE_DESERIALIZER_H_
#define SUPERSONIC_CONTRIB_STORAGE_CORE_DATA_TYPE_DESERIALIZER_H_

#include <stddef.h>
#include <endian.h>

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/bit_pointers.h"  // USE_BITS_...
#include "supersonic/contrib/storage/base/deserializer.h"
#include "supersonic/proto/supersonic.pb.h"


// TODO(wzoltak): Fix in future. Either ignore BigEndian or serialize
//                to bitmask by hand.
#ifndef IS_LITTLE_ENDIAN
#error "The storage serialization code supports little endian only"
#endif

#if USE_BITS_FOR_IS_NULL_REPRESENTATION == true
#error "The storage serialization code supports boolean nulls only"
#endif

namespace supersonic {

// Creates a Deserializer for given DataType.
FailureOrOwned<Deserializer> CreateDeserializer(DataType type,
                                                BufferAllocator* allocator);

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_CORE_DATA_TYPE_DESERIALIZER_H_
