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

#ifndef SUPERSONIC_CONTRIB_STORAGE_CORE_DATA_TYPE_SERIALIZER_H_
#define SUPERSONIC_CONTRIB_STORAGE_CORE_DATA_TYPE_SERIALIZER_H_

#include <stddef.h>

#include <iostream>
#include <bitset>

#include <endian.h>
#include <glog/logging.h>

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/contrib/storage/base/serializer.h"
#include "supersonic/contrib/storage/core/page_builder.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/utils/macros.h"
#include "supersonic/proto/supersonic.pb.h"

#include "supersonic/base/infrastructure/bit_pointers.h"

// TODO(wzoltak): Fix in future. Either ignore BigEndian or serialize
//                to bitmask by hand.
#ifndef IS_LITTLE_ENDIAN
#error "The storage serialization code supports little endian only"
#endif

#if USE_BITS_FOR_IS_NULL_REPRESENTATION == true
#error "The storage serialization code supports little endian only"
#endif



namespace supersonic {

namespace {
// TODO(wzoltak):
//  - string-piece serializer
//  - bitmask serializer

template <DataType T>
class NumericDataTypeSerializer : public Serializer {
 public:
  typedef typename TypeTraits<T>::cpp_type CppType;
  NumericDataTypeSerializer() {}

  FailureOrVoid Serialize(PageBuilder* output_buffer,
                          int output_stream,
                          VariantConstPointer data[],
                          const size_t lengths[],
                          const size_t arrays) {
    for (size_t array_index = 0; array_index < arrays; array_index ++) {
      const CppType* data_chunk = data[array_index].as<T>();
      output_buffer->AppendToByteBuffer(output_stream,
                                        data_chunk,
                                        lengths[array_index] * sizeof(CppType));
    }
    return Success();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(NumericDataTypeSerializer);
};

// TODO(wzoltak): is this useful?
typedef uint32_t Bitmask;
const size_t kBitsInBitmask = sizeof(Bitmask) * 8;
#define TO_LITTLE_ENDIAN(x) htole32(x)

// Serializes a C++ `bool` objects into bitmasks.
class CppBooleanSerializer : public Serializer {
 public:
  CppBooleanSerializer() {}

  FailureOrVoid Serialize(PageBuilder* output_buffer,
                          int output_stream,
                          VariantConstPointer data[],
                          const size_t lengths[],
                          const size_t arrays) {
    // TODO(wzoltak): Do not use vectors as buffer?
    std::vector<uint32_t> serialized_chunk;
    for (size_t array_index = 0; array_index < arrays; array_index ++) {
      const typename TypeTraits<BOOL>::cpp_type* data_chunk =
          data[array_index].as<BOOL>();
      uint32_t length = lengths[array_index];
      PackBooleans(data_chunk, length, &serialized_chunk);
      output_buffer->AppendToByteBuffer(output_stream, &serialized_chunk[0],
          serialized_chunk.size() * sizeof(uint32_t));
    }
    return Success();
  }

 private:
  void PackBooleans(const TypeTraits<BOOL>::cpp_type* data_chunk,
                    uint32_t length,
                    std::vector<uint32_t>* serialized_chunk) {
    size_t required_bitmasks = (length + kBitsInBitmask - 1) / kBitsInBitmask;
    serialized_chunk->clear();
    serialized_chunk->resize(required_bitmasks + 1, 0);

    (*serialized_chunk)[0] = length;

    // TODO(wzoltak): unroll loop?
    bit_pointer::bit_ptr serialized_data(&(*serialized_chunk)[1]);
    for (uint32_t index = 0; index < length; ++index, ++serialized_data) {
      *serialized_data |= data_chunk[index];
    }
  }

  DISALLOW_COPY_AND_ASSIGN(CppBooleanSerializer);
};

template <DataType T>
class VariableLengthSerializer : public Serializer {
 public:
  FailureOrVoid Serialize(PageBuilder* output_buffer,
                          int output_stream,
                          VariantConstPointer data[],
                          const size_t lengths[],
                          const size_t arrays) {
    for (size_t array_index = 0; array_index < arrays; array_index ++) {

      const StringPiece* string_pieces =
          data[array_index].as<T>();
      uint32_t pieces = lengths[array_index];
      output_buffer
          ->AppendToByteBuffer(output_stream, &pieces, sizeof(uint32_t));
      for (size_t i = 0; i < pieces; i++){
        const StringPiece& string_piece = string_pieces[i];
        uint32_t piece_length = string_piece.length();
        output_buffer->AppendToByteBuffer(output_stream,
                                          &piece_length,
                                          sizeof(uint32_t));
        output_buffer->AppendToByteBuffer(output_stream,
                                          string_piece.as_string().c_str(),
                                          piece_length);
      }
    }
    return Success();
  }
};

}  // namespace

inline FailureOrOwned<Serializer> CreateSerializer(DataType type) {
  switch(type) {
    case INT32:     return Success(new NumericDataTypeSerializer<INT32>());
    case INT64:     return Success(new NumericDataTypeSerializer<INT64>());
    case UINT32:    return Success(new NumericDataTypeSerializer<UINT32>());
    case UINT64:    return Success(new NumericDataTypeSerializer<UINT64>());
    case DATE:      return Success(new NumericDataTypeSerializer<DATE>());
    case DATETIME:  return Success(new NumericDataTypeSerializer<DATETIME>());
    case FLOAT:     return Success(new NumericDataTypeSerializer<FLOAT>());
    case DOUBLE:    return Success(new NumericDataTypeSerializer<DOUBLE>());
    case BOOL:      return Success(new CppBooleanSerializer());
    case BINARY:    return Success(new VariableLengthSerializer<BINARY>());
    case STRING:    return Success(new VariableLengthSerializer<STRING>());
    case ENUM:
    case DATA_TYPE:
        THROW(new Exception(
            ERROR_INVALID_ARGUMENT_TYPE,
            StringPrintf("Unable to create DataTypeSerializer for type %d",
                         type)));

  }

}


}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_CORE_DATA_TYPE_SERIALIZER_H_
