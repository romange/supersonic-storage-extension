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

#include "supersonic/contrib/storage/base/page.h"

#include <google/protobuf/io/coded_stream.h>

#include <memory>
#include <vector>

#include "supersonic/base/exception/result.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/memory/memory.h"

namespace supersonic {

typedef std::vector<struct ByteBufferHeader> ByteBufferHeaderVector;
typedef std::vector<const void*> ByteBufferVector;

// Represents a Page which does not own the data buffer.
class PageView : public Page {
 public:
  PageView(
      const Buffer& buffer,
      std::unique_ptr<struct PageHeader> page_header,
      std::unique_ptr<ByteBufferHeaderVector> byte_buffers_headers,
      std::unique_ptr<ByteBufferVector> byte_buffers)
          : buffer_(buffer), page_header_(std::move(page_header)),
              byte_buffers_headers_(std::move(byte_buffers_headers)),
              byte_buffers_(std::move(byte_buffers)) {}

  virtual ~PageView() {}

  const struct PageHeader& PageHeader() const {
    return *page_header_;
  }

  FailureOr<const struct ByteBufferHeader*> ByteBufferHeader(
      int byte_buffer_index) const {
    if (byte_buffer_index >= PageHeader().byte_buffers_count) {
      THROW(new Exception(
          ERROR_INVALID_ARGUMENT_VALUE,
          "Byte buffer index out of bounds."));
    }
    return Success(&(*byte_buffers_headers_)[byte_buffer_index]);
  }

  FailureOr<const void*> ByteBuffer(int byte_buffer_index) const {
    FailureOr<const struct ByteBufferHeader*> header =
        ByteBufferHeader(byte_buffer_index);
    PROPAGATE_ON_FAILURE(header);
    return Success((*byte_buffers_)[byte_buffer_index]);
  }

  const void* RawData() const {
    return buffer_.data();
  }

  bool IsEmpty() const {
    return PageHeader().byte_buffers_count == 0;
  }

 private:
  const Buffer& buffer_;
  std::unique_ptr<struct PageHeader> page_header_;
  std::unique_ptr<ByteBufferHeaderVector> byte_buffers_headers_;
  std::unique_ptr<ByteBufferVector> byte_buffers_;

  DISALLOW_COPY_AND_ASSIGN(PageView);
};

// Represents a Page which does own the raw data.
class PageImplementation : public PageView {
 public:
  PageImplementation(
      std::unique_ptr<const Buffer> buffer,
      std::unique_ptr<struct PageHeader> page_header,
      std::unique_ptr<ByteBufferHeaderVector> byte_buffers_headers,
      std::unique_ptr<ByteBufferVector> byte_buffers)
          : PageView(*buffer.get(), std::move(page_header),
              std::move(byte_buffers_headers),
              std::move(byte_buffers)),
            owned_buffer_(std::move(buffer)) {
  }

  virtual ~PageImplementation() {}

 private:
  std::unique_ptr<const Buffer> owned_buffer_;

  DISALLOW_COPY_AND_ASSIGN(PageImplementation);
};


namespace {

// Deserializes page header from raw data.
FailureOrVoid DeserializePageHeader(
    const uint8_t* page_data,
    const std::unique_ptr<struct PageHeader>& page_header) {
  ::google::protobuf::io::CodedInputStream input_stream(
      page_data, kSerializedPageHeaderSize);
  bool success = true;
  success = success && input_stream.ReadLittleEndian64(
      &page_header->total_size);
  success = success && input_stream.ReadLittleEndian32(
      &page_header->byte_buffers_count);

  if (!success) {
    THROW(new Exception(ERROR_GENERAL_IO_ERROR,
                        "Unable to deserialize page data."));
  }
  return Success();
}

// Deserializes byte buffer header from raw data.
FailureOrVoid DeserializeByteBufferHeader(
    const uint8_t* data,
    const std::unique_ptr<ByteBufferHeaderVector>& byte_buffers_headers) {
  ::google::protobuf::io::CodedInputStream input_stream(
      data, kSerializedByteBufferHeaderSize);
  struct ByteBufferHeader byte_buffer_header;
  if (!input_stream.ReadLittleEndian64(&byte_buffer_header.length)) {
    THROW(new Exception(ERROR_GENERAL_IO_ERROR,
                        "Unable to deserialize page data."));
  }
  byte_buffers_headers->push_back(byte_buffer_header);
  return Success();
}

FailureOrVoid DeserializeByteBuffers(
    const uint8_t* page_data,
    const std::unique_ptr<struct PageHeader>& page_header,
    const std::unique_ptr<ByteBufferHeaderVector>& byte_buffers_headers,
    const std::unique_ptr<ByteBufferVector>& byte_buffers) {
  ::google::protobuf::io::CodedInputStream input_stream(
      page_data + kSerializedPageHeaderSize,
      sizeof(Page::offset_t) * page_header->byte_buffers_count);
  for (uint32_t index = 0; index < page_header->byte_buffers_count; index++) {
    Page::offset_t byte_buffer_offset;
    if (!input_stream.ReadLittleEndian64(&byte_buffer_offset)) {
      THROW(new Exception(ERROR_GENERAL_IO_ERROR,
                          "Unable to deserialize page data."));
    }
    FailureOrVoid deserialized_byte_buffer_header = DeserializeByteBufferHeader(
        page_data + byte_buffer_offset, byte_buffers_headers);
    PROPAGATE_ON_FAILURE(deserialized_byte_buffer_header);

    byte_buffers->push_back(
        page_data + byte_buffer_offset + kSerializedByteBufferHeaderSize);
  }
  return Success();
}

// Helper function for Page creation. Use with caution.
template<typename PageClass, typename Buffer>
FailureOrOwned<Page> CreatePageImplementation(Buffer buffer,
                                              uint8_t* raw_data) {
  std::unique_ptr<struct PageHeader> page_header(new PageHeader());
  std::unique_ptr<ByteBufferHeaderVector> byte_buffers_headers(
      new ByteBufferHeaderVector());
  std::unique_ptr<ByteBufferVector> byte_buffers(new ByteBufferVector());

  FailureOrVoid deserialized_page_header =
      DeserializePageHeader(raw_data, page_header);
  PROPAGATE_ON_FAILURE(deserialized_page_header);

  FailureOrVoid deserialized_byte_buffers = DeserializeByteBuffers(
      raw_data, page_header, byte_buffers_headers, byte_buffers);
  PROPAGATE_ON_FAILURE(deserialized_byte_buffers);

  // Move `buffer` in case it is a std::unique_ptr.
  return Success(new PageClass(std::move(buffer),
                               std::move(page_header),
                               std::move(byte_buffers_headers),
                               std::move(byte_buffers)));
}


// Empty page, without underlying buffer.
// Throws on most operations.
class EmptyPage : public Page {
 public:
  EmptyPage() {
    header_.total_size = sizeof(struct PageHeader);
    header_.byte_buffers_count = 0;
  }

  ~EmptyPage() {}

  const struct PageHeader& PageHeader() const {
    return header_;
  }

  FailureOr<const struct ByteBufferHeader*>
        ByteBufferHeader(int byte_buffer_index) const {
    THROW(new Exception(ERROR_INVALID_ARGUMENT_TYPE,
                        "Accessing buffer from empty page."));
  }

  FailureOr<const void*> ByteBuffer(int byte_buffer_index) const {
    THROW(new Exception(ERROR_INVALID_ARGUMENT_TYPE,
                        "Accessing buffer from empty page."));
  }

  const void* RawData() const {
    return &header_;
  }

  bool IsEmpty() const {
    return true;
  }

 private:
  struct PageHeader header_;
};

}  // namespace

const Page* Page::EmptyPage() {
  static class EmptyPage page;
  return &page;
}

FailureOrOwned<Page> CreatePage(std::unique_ptr<const Buffer> buffer) {
  uint8_t* raw_data = static_cast<uint8_t*>(buffer->data());
  return CreatePageImplementation<PageImplementation,
                                  std::unique_ptr<const Buffer> >
      (std::move(buffer), raw_data);
}

FailureOrOwned<Page> CreatePageView(const Buffer& buffer) {
  uint8_t* raw_data = static_cast<uint8_t*>(buffer.data());
  return CreatePageImplementation<PageView, const Buffer&>(buffer, raw_data);
}

}  // namespace supersonic
