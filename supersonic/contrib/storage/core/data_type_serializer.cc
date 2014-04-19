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

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/contrib/storage/base/serializer.h"
#include "supersonic/contrib/storage/core/page_builder.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/utils/macros.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

// Serializer for common numeric types, like INT32 or DOUBLE. Simply copies the
// data into output buffer.
template <DataType T>
class NumericDataTypeSerializer : public Serializer {
 public:
  typedef typename TypeTraits<T>::cpp_type CppType;
  NumericDataTypeSerializer() {}

  FailureOrVoid Serialize(PageBuilder* output_page,
                          int output_buffer_number,
                          VariantConstPointer data[],
                          const size_t lengths[],
                          const size_t arrays) {
    for (size_t array_index = 0; array_index < arrays; array_index ++) {
      const CppType* data_chunk = data[array_index].as<T>();
      output_page->AppendToByteBuffer(output_buffer_number,
                                        data_chunk,
                                        lengths[array_index] * sizeof(CppType));
    }
    return Success();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(NumericDataTypeSerializer);
};

const size_t kBitsInBitmask = sizeof(uint32_t) * 8;

// Serializer for C++ `bool` type. Values are packed into 32bit bitmasks.
// The series of bitmasks is preceded by a count of packed values (not
// bitmasks!).
class CppBooleanSerializer : public Serializer {
 public:
  CppBooleanSerializer() {}

  FailureOrVoid Serialize(PageBuilder* output_page,
                          int output_buffer_number,
                          VariantConstPointer data[],
                          const size_t lengths[],
                          const size_t arrays) {
    for (size_t array_index = 0; array_index < arrays; array_index ++) {
      const typename TypeTraits<BOOL>::cpp_type* data_chunk =
          data[array_index].as<BOOL>();
      uint32_t length = lengths[array_index];
      size_t required_bitmasks = (length + kBitsInBitmask - 1) / kBitsInBitmask;
      size_t buffer_length = sizeof(uint32_t) * (required_bitmasks + 1);

      FailureOr<void*> buffer_result =
          output_page->NextFromByteBuffer(output_buffer_number, buffer_length);
      PROPAGATE_ON_FAILURE(buffer_result);
      uint32_t* buffer = static_cast<uint32_t*>(buffer_result.get());
      memset(buffer, 0, buffer_length);

      PackBooleans(data_chunk, length, buffer);
    }
    return Success();
  }

 private:
  void PackBooleans(const TypeTraits<BOOL>::cpp_type* data_chunk,
                    uint32_t length,
                    uint32_t* buffer) {
    buffer[0] = length;

    // TODO(wzoltak): unroll loop?
    bit_pointer::bit_ptr serialized_data(&buffer[1]);
    for (uint32_t index = 0; index < length; ++index, ++serialized_data) {
      *serialized_data |= data_chunk[index];
    }
  }

  DISALLOW_COPY_AND_ASSIGN(CppBooleanSerializer);
};

// Serializer for types which are using StringPiece as `cpp_type`.
//
// Data is stored in following format:
// +---------+---------+-----
// | Array_1 | Array_2 | ...
// +---------+---------+-----
//
// Array:
// +---------------------------+---------+---------+-----
// | uint32_t number_of_pieces | Piece_1 | Piece_2 | ...
// +---------------------------+---------+---------+-----
//
// Piece:
// +-----------------------+------------+------------+-----
// | uint32_t piece_length | uint8_t b1 | uint8_t b2 | ...
// +-----------------------+------------+------------+-----
//
template <DataType T>
class VariableLengthSerializer : public Serializer {
 public:
  VariableLengthSerializer() {}

  FailureOrVoid Serialize(PageBuilder* output_page,
                          int output_buffer_number,
                          VariantConstPointer data[],
                          const size_t lengths[],
                          const size_t arrays) {
    for (size_t array_index = 0; array_index < arrays; array_index ++) {
      const StringPiece* pieces = data[array_index].as<T>();
      uint32_t pieces_count = lengths[array_index];
      size_t buffer_length = RequiredBufferSize(pieces, pieces_count);

      FailureOr<void*> buffer_result =
          output_page->NextFromByteBuffer(output_buffer_number, buffer_length);
      PROPAGATE_ON_FAILURE(buffer_result);
      uint8_t* buffer = static_cast<uint8_t*>(buffer_result.get());

      SerializeRow(pieces, pieces_count, buffer);
    }
    return Success();
  }

 private:
  void SerializeRow(const StringPiece pieces[],
                    size_t pieces_count,
                    uint8_t* buffer) {
    *reinterpret_cast<uint32_t*>(buffer) = pieces_count;
    buffer += sizeof(uint32_t);
    for (size_t index = 0; index < pieces_count; index++) {
      const StringPiece& string_piece = pieces[index];
      uint32_t string_piece_length = string_piece.length();

      *reinterpret_cast<uint32_t*>(buffer) = string_piece_length;
      buffer += sizeof(uint32_t);

      memcpy(buffer, string_piece.as_string().c_str(), string_piece_length);
      buffer += string_piece_length;
    }
  }

  size_t RequiredBufferSize(const StringPiece pieces[], size_t count) {
    size_t lengths_sum = sizeof(uint32_t);
    for (int index = 0; index < count; index++) {
      lengths_sum += sizeof(uint32_t);
      lengths_sum += pieces[index].length();
    }
    return lengths_sum;
  }

  DISALLOW_COPY_AND_ASSIGN(VariableLengthSerializer);
};

FailureOrOwned<Serializer> CreateSerializer(DataType type) {
  switch (type) {
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
    default:
        THROW(new Exception(
            ERROR_INVALID_ARGUMENT_TYPE,
            StringPrintf("Unable to create DataTypeSerializer for type %d",
                         type)));
  }
}

}  // namespace supersonic
