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
#include "supersonic/contrib/storage/base/deserializer.h"
#include "supersonic/base/infrastructure/bit_pointers.h"


namespace supersonic {

namespace {

const size_t kDefaultDeserializerBufferSize = 1024;
const size_t kInitialArenaSize = 1024;
const size_t kMaxArenaSize = 60 * 1024 * 1024;  // 60MB

// Deserializer which owns memory buffer into which data is deserialized.
class OwningDeserializer : public Deserializer {
 public:
  OwningDeserializer(std::unique_ptr<Buffer> buffer, BufferAllocator* allocator)
      : buffer_(std::move(buffer)), allocator_(allocator) {}

  virtual ~OwningDeserializer() {}

 protected:
  FailureOrVoid MaybeResizeBuffer(size_t required_size) {
    if (buffer_->size() < required_size) {
      if (allocator_->Reallocate(required_size, buffer_.get()) == nullptr) {
        THROW(new Exception(ERROR_MEMORY_EXCEEDED,
                            "Unable to allocate memory for deserializer."));
      }
    }
    return Success();
  }

  std::unique_ptr<Buffer> buffer_;

 private:
  BufferAllocator* allocator_;
  DISALLOW_COPY_AND_ASSIGN(OwningDeserializer);
};

// Deserializer for common numeric types, like INT32 or DOUBLE. Simply copies
// the data into output buffer. Data must be copied instead of simply passing
// the pointer, since the source buffer may be invalidated and re-used for
// other deserializers.
// TODO(wzoltak): Handle different byte-SEXes?
template <DataType T>
class NumericDataTypeDeserializer : public OwningDeserializer {
 public:
  typedef typename TypeTraits<T>::cpp_type CppType;
  NumericDataTypeDeserializer(std::unique_ptr<Buffer> buffer,
                              BufferAllocator* allocator)
      : OwningDeserializer(std::move(buffer), allocator) {}

  virtual ~NumericDataTypeDeserializer() {}

  FailureOr<pair<VariantConstPointer, rowcount_t> >
      Deserialize(const void* byte_buffer,
                  const ByteBufferHeader& byte_buffer_header) {
    size_t row_count = byte_buffer_header.length / sizeof(CppType);

    PROPAGATE_ON_FAILURE(MaybeResizeBuffer(byte_buffer_header.length * 10000));
    memcpy(buffer_->data(), byte_buffer, byte_buffer_header.length);

    return Success(make_pair(buffer_->data(), row_count));
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(NumericDataTypeDeserializer);
};

const size_t kBitsInBitmask = sizeof(uint32_t) * 8;


// Deserializer for C++ `bool` type.
// See comments for CppBooleanSerializer for details on data format.
class CppBooleanDeserializer : public OwningDeserializer {
 public:
  CppBooleanDeserializer(std::unique_ptr<Buffer> buffer,
                         BufferAllocator* allocator)
      : OwningDeserializer(std::move(buffer), allocator) {}

  ~CppBooleanDeserializer() {}

  FailureOr<std::pair<VariantConstPointer, rowcount_t> >
      Deserialize(const void* byte_buffer,
                  const ByteBufferHeader& byte_buffer_header) {
    // Rough estimate - one bool per bit.
    PROPAGATE_ON_FAILURE(MaybeResizeBuffer(byte_buffer_header.length * 8));

    bool* result_buffer_beginning = static_cast<bool*>(buffer_->data());
    bool* dest_buffer = static_cast<bool*>(buffer_->data());
    const uint32_t* source_buffer = static_cast<const uint32_t*>(byte_buffer);
    const uint32_t* source_buffer_end =
        source_buffer + (byte_buffer_header.length / sizeof(uint32_t));

    while (source_buffer < source_buffer_end) {
      std::pair<const uint32_t*, bool*> unpack_result =
          UnpackBooleans(source_buffer, dest_buffer);
      source_buffer = unpack_result.first;
      dest_buffer = unpack_result.second;
    }

    VariantConstPointer pointer_result(result_buffer_beginning);
    rowcount_t row_count = dest_buffer - result_buffer_beginning;
    return Success(std::make_pair(pointer_result, row_count));
  }

 private:
  std::pair<const uint32_t*, bool*>
      UnpackBooleans(const uint32_t* source_buffer, bool* dest_buffer) {
    const uint32_t length = source_buffer[0];
    bit_pointer::bit_ptr serialized_data(
        const_cast<uint32_t*>(&source_buffer[1]));
    for (uint32_t index = 0; index < length;
        index++, dest_buffer++, ++serialized_data) {
      bool value = (*serialized_data);
      *dest_buffer = value;
    }
    uint32_t processed = 1 + (length + kBitsInBitmask - 1) / kBitsInBitmask;
    source_buffer += processed;
    return std::make_pair(source_buffer, dest_buffer);
  }

  DISALLOW_COPY_AND_ASSIGN(CppBooleanDeserializer);
};


// Deserializer for types which are using StringPiece as `cpp_type`.
//
// For details on data format, please see comment for VariableLengthSerializer.
template <DataType T>
class VariableLengthDeserializer : public OwningDeserializer {
 public:
  VariableLengthDeserializer(std::unique_ptr<Buffer> buffer,
                             BufferAllocator* allocator)
     : OwningDeserializer(std::move(buffer), allocator),
       arena_(new Arena(allocator, kInitialArenaSize, kMaxArenaSize)) {}

  FailureOr<std::pair<VariantConstPointer, rowcount_t> >
      Deserialize(const void* byte_buffer,
                  const ByteBufferHeader& byte_buffer_header) {
    arena_->Reset();

    const uint8_t* source_buffer = static_cast<const uint8_t*>(byte_buffer);
    const uint8_t* source_buffer_end =
        source_buffer + byte_buffer_header.length;
    size_t required_size =
        sizeof(StringPiece) * CountRows(source_buffer, source_buffer_end);
    PROPAGATE_ON_FAILURE(MaybeResizeBuffer(required_size));

    StringPiece* result_buffer_beginning;
    StringPiece* dest_buffer;
    result_buffer_beginning = dest_buffer =
        static_cast<StringPiece*>(buffer_->data());

    while (source_buffer < source_buffer_end) {
      FailureOr<std::pair<const uint8_t*, StringPiece*>> deserialize_result =
          DeserializeRow(source_buffer, dest_buffer);
      PROPAGATE_ON_FAILURE(deserialize_result);
      source_buffer = deserialize_result.get().first;
      dest_buffer = deserialize_result.get().second;
    }

    VariantConstPointer pointer_result(result_buffer_beginning);
    rowcount_t row_count = dest_buffer - result_buffer_beginning;
    return Success(std::make_pair(pointer_result, row_count));
  }

 private:
  FailureOr<std::pair<const uint8_t*, StringPiece*>>
      DeserializeRow(const uint8_t* source_buffer, StringPiece* dest_buffer) {
    const uint32_t length = *reinterpret_cast<const uint32_t*>(source_buffer);
    fflush(stdout);
    source_buffer += sizeof(uint32_t);

    for (uint32_t index = 0; index < length; index++, dest_buffer++) {
      const uint32_t piece_length =
          *reinterpret_cast<const uint32_t*>(source_buffer);
      source_buffer += sizeof(uint32_t);

      char* raw_piece = static_cast<char*>(arena_->AllocateBytes(piece_length));
      if (raw_piece == nullptr) {
        THROW(new Exception(ERROR_MEMORY_EXCEEDED,
                            "Unable to allocate memory for deserializer."));
      }
      memcpy(raw_piece, source_buffer, piece_length);
      dest_buffer->set(raw_piece, piece_length);

      source_buffer += piece_length;
    }

    return Success(std::make_pair(source_buffer, dest_buffer));
  }

  size_t CountRows(const uint8_t* buffer_it, const uint8_t* buffer_end) {
    size_t num_rows = 0;

    while (buffer_it < buffer_end) {
      uint32_t pieces = *reinterpret_cast<const uint32_t*>(buffer_it);
      buffer_it += sizeof(uint32_t);
      for (uint32_t index = 0; index < pieces; index++) {
        const uint32_t piece_length =
            *reinterpret_cast<const uint32_t*>(buffer_it);
        buffer_it += sizeof(uint32_t) + piece_length;
      }
      num_rows += pieces;
    }
    return num_rows;
  }

  std::unique_ptr<Arena> arena_;
};

}  // namespace

FailureOrOwned<Deserializer> CreateDeserializer(DataType type,
                                                BufferAllocator* allocator) {
  std::unique_ptr<Buffer>
      buffer(allocator->Allocate(kDefaultDeserializerBufferSize));
  if (buffer.get() == nullptr) {
    THROW(new Exception(ERROR_MEMORY_EXCEEDED,
                        "Unable to allocate buffer for deserializer"));
  }

  switch (type) {
    case INT32:     return Success(
        new NumericDataTypeDeserializer<INT32>(std::move(buffer), allocator));
    case INT64:     return Success(
        new NumericDataTypeDeserializer<INT64>(std::move(buffer), allocator));
    case UINT32:    return Success(
        new NumericDataTypeDeserializer<UINT32>(std::move(buffer), allocator));
    case UINT64:    return Success(
        new NumericDataTypeDeserializer<UINT64>(std::move(buffer), allocator));
    case DATE:      return Success(
        new NumericDataTypeDeserializer<DATE>(std::move(buffer), allocator));
    case DATETIME:  return Success(
        new NumericDataTypeDeserializer<DATETIME>(std::move(buffer),
                                                  allocator));
    case FLOAT:     return Success(
        new NumericDataTypeDeserializer<FLOAT>(std::move(buffer), allocator));
    case DOUBLE:    return Success(
        new NumericDataTypeDeserializer<DOUBLE>(std::move(buffer), allocator));
    case BOOL:      return Success(
        new CppBooleanDeserializer(std::move(buffer), allocator));
    case BINARY:    return Success(
        new VariableLengthDeserializer<BINARY>(std::move(buffer), allocator));
    case STRING:    return Success(
        new VariableLengthDeserializer<STRING>(std::move(buffer), allocator));
    case ENUM:
    case DATA_TYPE:
    default:
        THROW(new Exception(
            ERROR_INVALID_ARGUMENT_TYPE,
            StringPrintf("Unable to create DataTypeDeserializer for type %d",
                         type)));
  }
}

}  // namespace supersonic
