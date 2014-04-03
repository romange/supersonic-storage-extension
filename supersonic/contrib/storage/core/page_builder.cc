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

#include "supersonic/contrib/storage/core/page_builder.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <string.h>
#include <memory>
#include <vector>

#include "supersonic/base/memory/arena.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/contrib/storage/base/page.h"

namespace supersonic {

const size_t kMaxPageSize = 1024 * 1024;  // 1MB

// Internal implementation of PageBuilder class.
// TODO(wzoltak): What about buffers alignment?
class PageBuilder::Implementation {
 public:
  Implementation(const PageBuilder& pub, BufferAllocator* allocator)
      : pub_(pub), page_size_(0), allocator_(allocator),
        arena_(std::unique_ptr<Arena>(new Arena(allocator, 0, kMaxPageSize))) {
  }

  // Returns a size of headers in page.
  size_t HeadersSize() {
    size_t header_size = kSerializedPageHeaderSize +
        sizeof(Page::offset_t) * pub_.ByteBuffersCount();
    size_t byte_buffers_headers_size =
        kSerializedByteBufferHeaderSize * pub_.ByteBuffersCount();
    return header_size + byte_buffers_headers_size;
  }

  // Writes PageHeader representing current state into destination.
  void WritePageHeader(uint8_t* dest) {
    google::protobuf::io::CodedOutputStream output_stream(
        new google::protobuf::io::ArrayOutputStream(
            dest, kSerializedPageHeaderSize));
    output_stream.WriteLittleEndian64(pub_.PageSize());
    output_stream.WriteLittleEndian32(pub_.ByteBuffersCount());
  }

  // Writes selected buffer representing current state into destination.
  size_t WriteByteBuffer(int byte_buffer_index, uint8_t* dest) {
    uint8_t* data = dest + kSerializedByteBufferHeaderSize;
    uint64_t length = 0;
    for (int i = 0; i < byte_buffer_chunks_[byte_buffer_index].size(); i++) {
      size_t chunk_length = byte_buffer_chunks_lengths_[byte_buffer_index][i];
      memcpy(data + length,
             byte_buffer_chunks_[byte_buffer_index][i],
             chunk_length);
      length += chunk_length;
    }

    google::protobuf::io::CodedOutputStream output_stream(
        new google::protobuf::io::ArrayOutputStream(
            dest, kSerializedByteBufferHeaderSize));
    output_stream.WriteLittleEndian64(length);

    return length + kSerializedByteBufferHeaderSize;
  }

  const PageBuilder& pub_;
  uint64_t page_size_;
  std::vector<std::vector<void*> > byte_buffer_chunks_;
  std::vector<std::vector<uint64_t> > byte_buffer_chunks_lengths_;

  BufferAllocator* allocator_;
  std::unique_ptr<Arena> arena_;
};

PageBuilder::PageBuilder(uint32_t byte_buffers_count,
                         BufferAllocator* allocator) {
  implementation_= std::unique_ptr<Implementation>(
      new Implementation(*this, allocator));
  Reset(byte_buffers_count);
}

PageBuilder::~PageBuilder() {}

uint32_t PageBuilder::ByteBuffersCount() const {
  return implementation_->byte_buffer_chunks_.size();
}

uint64_t PageBuilder::PageSize() const {
  return implementation_->page_size_;
}

void PageBuilder::Reset() {
  Reset(ByteBuffersCount());
}

void PageBuilder::Reset(unsigned int byte_buffers_count) {
  implementation_->arena_->Reset();
  implementation_->byte_buffer_chunks_.clear();
  implementation_->byte_buffer_chunks_lengths_.clear();
  implementation_->byte_buffer_chunks_.resize(byte_buffers_count);
  implementation_->byte_buffer_chunks_lengths_.resize(byte_buffers_count);
  implementation_->page_size_ = implementation_->HeadersSize();
}

FailureOrVoid PageBuilder::AppendToByteBuffer(
    unsigned int byte_buffer_index,
    const void* data,
    size_t length) {
  FailureOr<void*> buffer_result =
      NextFromByteBuffer(byte_buffer_index, length);
  PROPAGATE_ON_FAILURE(buffer_result);

  memcpy(buffer_result.get(), data, length);
  return Success();
}

FailureOr<void*> PageBuilder::NextFromByteBuffer(
    unsigned int byte_buffer_index,
    size_t length) {
  if (byte_buffer_index >= ByteBuffersCount()) {
      THROW(new Exception(
          ERROR_INVALID_ARGUMENT_VALUE,
          "Byte buffer index out of bounds."));
  }

  void* storage = implementation_->arena_->AllocateBytes(length);
  if (storage == NULL) {
    THROW(new Exception(
        ERROR_MEMORY_EXCEEDED,
        "Can't allocate memory for byte buffer chunk."));
  }

  implementation_->byte_buffer_chunks_[byte_buffer_index].push_back(storage);
  implementation_->byte_buffer_chunks_lengths_[byte_buffer_index]
      .push_back(length);
  implementation_->page_size_ += length;

  return Success(storage);
}

FailureOrOwned<Page> PageBuilder::CreatePage() {
  std::unique_ptr<Buffer> data_buffer(
      implementation_->allocator_->Allocate(PageSize()));
  uint8_t* data = static_cast<uint8_t*>(data_buffer->data());

  implementation_->WritePageHeader(data);

  google::protobuf::io::CodedOutputStream output_stream(
      new google::protobuf::io::ArrayOutputStream(
          data + kSerializedPageHeaderSize,
          sizeof(Page::offset_t) * ByteBuffersCount()));

  Page::offset_t byte_buffer_offset = kSerializedPageHeaderSize +
      ByteBuffersCount() * sizeof(Page::offset_t);
  for (unsigned int index = 0; index < ByteBuffersCount(); index++) {
    output_stream.WriteLittleEndian64(byte_buffer_offset);
    byte_buffer_offset +=
        implementation_->WriteByteBuffer(index, data + byte_buffer_offset);
  }

  FailureOrOwned<Page> page = ::supersonic::CreatePage(std::move(data_buffer));
  PROPAGATE_ON_FAILURE(page);
  return Success(page.release());
}

}  // namespace supersonic
