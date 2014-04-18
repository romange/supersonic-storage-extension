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

#ifndef SUPERSONIC_CONTRIB_STORAGE_CORE_FILE_STORAGE_H_
#define SUPERSONIC_CONTRIB_STORAGE_CORE_FILE_STORAGE_H_

#include <glog/logging.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <set>
#include <string>

#include "supersonic/base/exception/result.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/contrib/storage/base/byte_stream_writer.h"
#include "supersonic/contrib/storage/base/page.h"
#include "supersonic/contrib/storage/base/page_stream_writer.h"
#include "supersonic/contrib/storage/base/storage.h"
#include "supersonic/contrib/storage/core/page_builder.h"
#include "supersonic/utils/file.h"
#include "supersonic/utils/stringprintf.h"


namespace supersonic {

const size_t kInitialPageReaderBuffer = 1024 * 1024;  // 1MB

// Storage which stores data in files, operating via the supersonic::File
// interface.
template<class FileT>
class FileStorage : public Storage {
 public:
  explicit FileStorage(const std::string& path,
                       BufferAllocator* buffer_allocator)
      : path_(path), buffer_allocator_(buffer_allocator) {
    static_assert(std::is_base_of<File, FileT>::value,
        "Not a supersonic::File interface implementation!");
  }

  virtual FailureOrOwned<PageStreamWriter> CreatePageStreamWriter(
      const std::string& name) {
    FailureOrOwned<File> file = OpenFileForWriting(name);
    PROPAGATE_ON_FAILURE(file);
    return Success(new FilePageStreamWriter(file.release()));
  }

  virtual FailureOrOwned<ByteStreamWriter> CreateByteStreamWriter(
      const std::string& name) {
    FailureOrOwned<File> file = OpenFileForWriting(name);
    PROPAGATE_ON_FAILURE(file);
    return Success(new FileByteStreamWriter(file.release()));
  }

  virtual FailureOrOwned<PageStreamReader> CreatePageStreamReader(
      const std::string& name) {
    PageBuilder empty_page_builder(0, buffer_allocator_);
    FailureOrOwned<Page> empty_page_result = empty_page_builder.CreatePage();
    PROPAGATE_ON_FAILURE(empty_page_result);
    std::unique_ptr<Page> empty_page(empty_page_result.release());

    std::unique_ptr<Buffer> buffer(
        buffer_allocator_->Allocate(kInitialPageReaderBuffer));
    if (buffer->data() == NULL) {
      THROW(new Exception(
            ERROR_MEMORY_EXCEEDED,
            "Can not allocate enough memory for PageStreamReader buffer."));
    }

    FailureOrOwned<File> file = OpenFileForReading(name);
    PROPAGATE_ON_FAILURE(file);

    return Success(new FilePageStreamReader(file.release(),
                                            std::move(buffer),
                                            buffer_allocator_,
                                            std::move(empty_page)));


    THROW(new Exception(ERROR_NOT_IMPLEMENTED, "Not implemented."));
  }

  virtual FailureOrOwned<ByteStreamReader> CreateByteStreamReader(
      const std::string& name) {
    FailureOrOwned<File> file_result = OpenFileForReading(name);
    PROPAGATE_ON_FAILURE(file_result);
    return Success(new FileByteStreamReader(file_result.release()));
  }

 private:
  // Opens file with given name, in storage, for writing. If file exists, its
  // contents would be removed.
  FailureOrOwned<File> OpenFileForWriting(const std::string& stream_name) {
    return OpenFileForStream(stream_name, "w+");
  }

  // Opens file with given name, in storage, for reading.
  FailureOrOwned<File> OpenFileForReading(const std::string& stream_name) {
    return OpenFileForStream(stream_name, "r");
  }

  // Opens file with given name in given mode, in storage.
  FailureOrOwned<File> OpenFileForStream(const std::string& stream_name,
                                         const std::string& mode) {
    if (streams_.find(stream_name) != streams_.end()) {
      THROW(new Exception(
          ERROR_INVALID_ARGUMENT_VALUE,
          StringPrintf("Stream %s already exists.", stream_name.c_str())));
    }

    const std::string stream_path = FileT::JoinPath(path_, stream_name);
    File* file = FileT::OpenOrDie(stream_path, mode);
    if (file == nullptr) {
      // TODO(wzoltak): Actually, there is not guarantee that errno will be set.
      THROW(new Exception(ERROR_GENERAL_IO_ERROR, strerror(errno)));
    }
    return Success(file);
  }

  const std::string path_;
  std::set<std::string> streams_;
  BufferAllocator* buffer_allocator_;

  // Internal ByteStreamWriter implementation for FileStorage.
  class FileByteStreamWriter : public ByteStreamWriter {
   public:
    explicit FileByteStreamWriter(File* file) : file_(file), finalized_(false),
        stream_path_(file_->CreateFileName()) {}

    virtual ~FileByteStreamWriter() {
      if (!finalized_) {
        LOG(DFATAL) << "Destroying not finalized FileByteStream.";
        Finalize();
      }
    }

    virtual FailureOrVoid AppendBytes(const void* buffer, size_t length) {
      if (finalized_) {
        THROW(new Exception(
            ERROR_INVALID_STATE,
            StringPrintf("Writing to finalized stream under '%s'.",
                         stream_path_.c_str())));
      }

      int64 written_bytes = file_->Write(buffer, length);
      if (written_bytes != length) {
        THROW(new Exception(
            ERROR_GENERAL_IO_ERROR,
            StringPrintf("Incomplete write to stream under '%s'.",
                         stream_path_.c_str())));
      }
      return Success();
    }

    virtual FailureOrVoid Finalize() {
      if (!file_->Close()) {
        THROW(new Exception(
            ERROR_GENERAL_IO_ERROR,
            StringPrintf("Can not close the underlying file '%s'.",
                         stream_path_.c_str())));
      }
      finalized_ = true;
      return Success();
    }

   private:
    std::unique_ptr<FileByteStreamWriter> byte_stream_;
    File* file_;
    bool finalized_;
    const std::string stream_path_;
    DISALLOW_COPY_AND_ASSIGN(FileByteStreamWriter);
  };

  // Internal PageStreamWriter implementation for FileStorage.
  class FilePageStreamWriter : public PageStreamWriter {
   public:
    explicit FilePageStreamWriter(File* file) : byte_stream_(file) {}

    virtual FailureOrVoid AppendPage(const Page& page) {
      FailureOrVoid appended = byte_stream_.AppendBytes(page.RawData(),
          page.PageHeader().total_size);
      PROPAGATE_ON_FAILURE(appended);
      return Success();
    }

    virtual FailureOrVoid Finalize() {
      FailureOrVoid finalized = byte_stream_.Finalize();
      PROPAGATE_ON_FAILURE(finalized);
      return Success();
    }

   private:
    FileByteStreamWriter byte_stream_;
    DISALLOW_COPY_AND_ASSIGN(FilePageStreamWriter);
  };

  // Internal ByteStreamReader implementation for FileStorage.
  class FileByteStreamReader : public ByteStreamReader {
   public:
    explicit FileByteStreamReader(File* file) : file_(file), finalized_(false),
        stream_path_(file_->CreateFileName()) {}

    virtual ~FileByteStreamReader() {
      if (!finalized_) {
        LOG(DFATAL) << "Destroying not finalized FileByteStreamReader.";
        Finalize();
      }
    }

    virtual FailureOr<int64_t> ReadBytes(void* buffer, int64_t size) {
      if (finalized_) {
        THROW(new Exception(
            ERROR_INVALID_STATE,
            StringPrintf("Reading from finalized stream under '%s'.",
                         stream_path_.c_str())));
      }

      int64_t read_bytes = file_->Read(buffer, size);
      if (read_bytes < 0) {
        THROW(new Exception(
            ERROR_GENERAL_IO_ERROR,
            StringPrintf("Error during read from underlying file '%s'.",
                         stream_path_.c_str())));
      }
      return Success(read_bytes);
    }

    virtual FailureOrVoid Finalize() {
      if (!file_->Close()) {
        THROW(new Exception(
            ERROR_GENERAL_IO_ERROR,
            StringPrintf("Can not close the underlying file '%s'.",
                         stream_path_.c_str())));
      }
      finalized_ = true;
      return Success();
    }

   private:
    File* file_;
    bool finalized_;
    const std::string stream_path_;
    DISALLOW_COPY_AND_ASSIGN(FileByteStreamReader);
  };

  // Internal PageStreamReader implementation for FileStorage.
  class FilePageStreamReader : public PageStreamReader {
   public:
    explicit FilePageStreamReader(File* file,
                                  std::unique_ptr<Buffer> buffer,
                                  BufferAllocator* buffer_allocator,
                                  std::unique_ptr<Page> empty_page)
        : byte_stream_(file), next_page_size_(0), buffer_(std::move(buffer)),
          buffer_allocator_(buffer_allocator),
          empty_page_(std::move(empty_page)) {}

    virtual FailureOr<const Page*> NextPage() {
      FailureOrVoid maybe_read_first_page_size_result =
          MaybeReadFirstPageSize();
      PROPAGATE_ON_FAILURE(maybe_read_first_page_size_result);
      if (next_page_size_ == 0) {
        return Success(empty_page_.get());
      }

      size_t required_size = next_page_size_ + sizeof(uint64_t);
      FailureOrVoid maybe_resize_result = MaybeResizeBuffer(required_size);
      PROPAGATE_ON_FAILURE(maybe_resize_result);

      FailureOrVoid read_result = ReadNextPage();
      PROPAGATE_ON_FAILURE(read_result);

      return Success(read_page_.get());
    }

    virtual FailureOrVoid Finalize() {
      FailureOrVoid finalized = byte_stream_.Finalize();
      PROPAGATE_ON_FAILURE(finalized);
      return Success();
    }

   private:
    // If next page size is not known retrieves it from stream. Otherwise,
    // does nothing.
    FailureOrVoid MaybeReadFirstPageSize() {
      // TODO(wzoltak): That looks a bit too tricky, simplify?
      if (next_page_size_ == 0) {
        FailureOr<int64_t> read_result =
            byte_stream_.ReadBytes(&next_page_size_, sizeof(uint64_t));
        PROPAGATE_ON_FAILURE(read_result);
        if (read_result.get() == 0) {
          next_page_size_ = 0;
        } else if (read_result.get() != sizeof(uint64_t)) {
          THROW(new Exception(ERROR_GENERAL_IO_ERROR,
                              "Error while reading next page size."));
        }
      }
      return Success();
    }

    // Resizes the buffer if it is too small to contain the next page.
    FailureOrVoid MaybeResizeBuffer(size_t required_size) {
      if (buffer_->size() < required_size) {
        buffer_allocator_->Reallocate(required_size, buffer_.get());
        if (buffer_->data() == NULL) {
          THROW(new Exception(
              ERROR_MEMORY_EXCEEDED,
              "Can not allocate enough memory for PageStreamReader buffer."));
        }
      }
      return Success();
    }

    // Reads page contents from byte stream, fills length field and sets
    // the length of next page.
    FailureOrVoid ReadNextPage() {
      uint8_t* contents =
          static_cast<uint8_t*>(buffer_->data()) + sizeof(uint64_t);
      FailureOr<int64_t> read_result =
          byte_stream_.ReadBytes(contents, next_page_size_);
      PROPAGATE_ON_FAILURE(read_result);
      int64_t read_bytes = read_result.get();

      *static_cast<uint64_t*>(buffer_->data()) = next_page_size_;

      if (!ReadEnough(read_bytes)) {
        THROW(new Exception(ERROR_GENERAL_IO_ERROR,
                            "Error while reading page contents."));
      }

      FailureOrOwned<Page> page_result = CreatePageView(*buffer_.get());
      PROPAGATE_ON_FAILURE(page_result);
      read_page_.reset(page_result.release());

      if (ReadWholePage(read_bytes)) {
        UpdateNextPageSize();
      } else {
        next_page_size_ = 0;
      }

      return Success();
    }

    int64_t PageSizeWithoutLengthField() {
      return next_page_size_ - sizeof(uint64_t);
    }

    bool ReadEnough(int64_t read_bytes) {
      return ReadWholePage(read_bytes) || ReadLastPage(read_bytes);
    }

    bool ReadWholePage(int64_t read_bytes) {
      return read_bytes == next_page_size_;
    }

    bool ReadLastPage(int64_t read_bytes) {
      return read_bytes == PageSizeWithoutLengthField();
    }

    void UpdateNextPageSize() {
      const void* size_ptr = static_cast<const uint8_t*>(
              buffer_->data()) + next_page_size_;
      next_page_size_ = *static_cast<const uint64_t*>(size_ptr);
    }

    FileByteStreamReader byte_stream_;
    uint64_t next_page_size_;
    std::unique_ptr<Buffer> buffer_;
    BufferAllocator* buffer_allocator_;
    std::unique_ptr<Page> empty_page_;
    std::unique_ptr<Page> read_page_;

    DISALLOW_COPY_AND_ASSIGN(FilePageStreamReader);
  };

  DISALLOW_COPY_AND_ASSIGN(FileStorage);
};

template<class FileT, class PathUtilT>
FailureOrOwned<Storage> CreateFileStorage(const std::string& path,
                                          BufferAllocator* buffer_allocator) {
  if (!PathUtilT::ExistsDir(path)) {
    // TODO(wzoltak): Good mode?
    bool created = PathUtilT::MkDir(path, S_IRWXU, true /* with_parents */);
    if (!created) {
      THROW(new Exception(
          ERROR_GENERAL_IO_ERROR,
          StringPrintf("Unable to create directory '%s' for FileStorage.",
                       path.c_str())));
    }
  } else {
    THROW(new Exception(
        ERROR_GENERAL_IO_ERROR,
        StringPrintf("Directory '%s' already exists.", path.c_str())));
  }
  return Success(new FileStorage<FileT>(path, buffer_allocator));
}

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_CORE_FILE_STORAGE_H_
