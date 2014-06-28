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
//
// Classes for file-based persistent storage. Currently, only the unix file
// system is supported. It is possible to integrate other file systems,
// e.g. distributed FS like HDFS, by creating an implementation of File and
// PathUtil interfaces.
//
// No two storage objects for single location should be active at the same time
// (either Writable or Readable).

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
#include "supersonic/contrib/storage/base/random_page_reader.h"
#include "supersonic/contrib/storage/base/storage.h"
#include "supersonic/contrib/storage/core/page_builder.h"
#include "supersonic/contrib/storage/core/file_series.h"
#include "supersonic/utils/file.h"
#include "supersonic/utils/stringprintf.h"


namespace supersonic {

const size_t kInitialPageReaderBuffer = 1024 * 1024;  // 1MB


// Opens file under given path with given mode.
template<class FileT>
FailureOrOwned<File> OpenFileWithMode(const std::string& path,
                                      const std::string& mode) {
  static_assert(std::is_base_of<File, FileT>::value,
                "Not a supersonic::File interface implementation!");

  File* file = FileT::OpenOrDie(path, mode);
  if (file == nullptr) {
    // TODO(wzoltak): Actually, there is not guarantee that errno will be set.
    THROW(new Exception(ERROR_GENERAL_IO_ERROR, strerror(errno)));
  }
  return Success(file);
}


// ByteStreamWriter which writes to given File instance.
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


// PageStreamWriter, which writes to given File instance.
class FilePageStreamWriter : public PageStreamWriter {
 public:
  explicit FilePageStreamWriter(File* file)
      : byte_stream_(file),
        written_pages_(0) {}

  virtual FailureOrVoid AppendPage(const Page& page) {
    FailureOrVoid appended = byte_stream_.AppendBytes(page.RawData(),
        page.PageHeader().total_size);
    PROPAGATE_ON_FAILURE(appended);
    written_pages_++;
    return Success();
  }

  virtual FailureOrVoid Finalize() {
    PROPAGATE_ON_FAILURE(
        byte_stream_.AppendBytes(&written_pages_, sizeof(uint64_t)));
    FailureOrVoid finalized = byte_stream_.Finalize();
    PROPAGATE_ON_FAILURE(finalized);
    return Success();
  }

 private:
  FileByteStreamWriter byte_stream_;
  uint64_t written_pages_;
  DISALLOW_COPY_AND_ASSIGN(FilePageStreamWriter);
};


// WritableStorage which stores data in files, operating via the
// supersonic::File interface.
template<class FileT>
class WritableFileStorage : public WritableStorage {
 public:
  WritableFileStorage(std::unique_ptr<FileSeries> file_series,
                      BufferAllocator* allocator)
      : file_series_(std::move(file_series)) {}

  virtual ~WritableFileStorage() {}

  FailureOrOwned<PageStreamWriter> NextPageStreamWriter() {
    return CreatePageStreamWriter(file_series_->NextFileName());
  }

 private:
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

  // Opens file with given name, in storage, for writing. If file exists, its
  // contents would be removed.
  FailureOrOwned<File> OpenFileForWriting(const std::string& stream_name) {
    return OpenFileWithMode<FileT>(stream_name, "w+");
  }

  std::unique_ptr<FileSeries> file_series_;

  DISALLOW_COPY_AND_ASSIGN(WritableFileStorage);
};


class FileRandomPageReader : public RandomPageReader {
 public:
  FileRandomPageReader(File* file,
                       std::unique_ptr<Buffer> buffer,
                       BufferAllocator* allocator)
     : file_(file),
       buffer_(std::move(buffer)),
       allocator_(allocator),
       total_pages_(0),
       finalized_(false) {}

  virtual ~FileRandomPageReader() {
    if (!finalized_) {
      LOG(DFATAL) << "Destroying not finalized FileRandomPageReader.";
      Finalize();
    }
  }

  // Inits the instances. Must be called before any other method.
  FailureOrVoid Init() {
    int64_t file_size = file_->Size();
    if (file_size < 0) {
      THROW(new Exception(
          ERROR_GENERAL_IO_ERROR,
          "Unable to retrieve total size of underlying file."));
    }
    ReadExactly(file_size - sizeof(uint64_t),
                &total_pages_,
                sizeof(uint64_t));
    return Success();
  }

  FailureOr<const Page*> GetPage(int offset) {
    if (finalized_) {
      THROW(new Exception(
          ERROR_INVALID_STATE,
          "Reading from finalized FileRandomPageReader."));
    }
    uint64_t page_size;
    PROPAGATE_ON_FAILURE(ReadExactly(offset, &page_size, sizeof(uint64_t)));
    MaybeResizeBuffer(page_size);

    PROPAGATE_ON_FAILURE(ReadExactly(offset, buffer_->data(), page_size));

    FailureOrOwned<Page> page_result = CreatePageView(*buffer_.get());
    PROPAGATE_ON_FAILURE(page_result);
    read_page_.reset(page_result.release());

    return Success(read_page_.get());
  }

  virtual uint64_t TotalPages() {
    return total_pages_;
  }

  virtual FailureOrVoid Finalize() {
    if (!finalized_ && !file_->Close()) {
      THROW(new Exception(
          ERROR_GENERAL_IO_ERROR,
          "Can not close the underlying file."));
    }
    finalized_ = true;
    return Success();
  }

 private:
  FailureOrVoid MaybeResizeBuffer(uint64_t required_size) {
    if (buffer_->size() < required_size) {
      allocator_->Reallocate(required_size, buffer_.get());
      if (buffer_->data() == NULL) {
        THROW(new Exception(
            ERROR_MEMORY_EXCEEDED,
            "Can not allocate enough memory for RandomPageReader buffer."));
      }
    }
    return Success();
  }

  FailureOrVoid ReadExactly(int64 offset, void* buffer, size_t length) {
    // TODO(wzoltak): Indicate which file in exceptions.
    if (!file_->Seek(offset)) {
      THROW(new Exception(
          ERROR_GENERAL_IO_ERROR,
          "Unable to seek through underlying file."));
    }

    uint8_t* byte_buffer = static_cast<uint8_t*>(buffer);
    do {
      int64 read_bytes = file_->Read(byte_buffer, length);
      if (read_bytes < 0) {
        THROW(new Exception(
            ERROR_GENERAL_IO_ERROR,
            "Error during read from underlying file"));
      } else if (read_bytes == 0) {
        THROW(new Exception(ERROR_GENERAL_IO_ERROR, "Unexpected end of file"));
      }
      byte_buffer += read_bytes;
      length -= read_bytes;
    } while (length > 0);

    return Success();
  }

  File* file_;
  std::unique_ptr<Buffer> buffer_;
  BufferAllocator* allocator_;
  std::unique_ptr<Page> read_page_;
  uint64_t total_pages_;
  bool finalized_;
  DISALLOW_COPY_AND_ASSIGN(FileRandomPageReader);
};


// ReadableStorage which reads data from files, operating via the
// supersonic::File interface.
template<class FileT>
class ReadableFileStorage : public ReadableStorage {
 public:
  explicit ReadableFileStorage(std::unique_ptr<FileSeries> file_series,
                               BufferAllocator* allocator)
      : allocator_(allocator),
        file_name_generator_(std::move(file_series)),
        next_name_(file_name_generator_->NextFileName()) {}

  FailureOrOwned<RandomPageReader> NextRandomPageReader() {
    FailureOrOwned<RandomPageReader> result =
        CreateRandomPageReader(next_name_);
    next_name_ = file_name_generator_->NextFileName();
    return result;
  }

  bool HasNext() {
    return FileT::Exists(FileT::JoinPath(path_, next_name_));
  }

 private:
  virtual FailureOrOwned<RandomPageReader> CreateRandomPageReader(
      const std::string& name) {
    std::unique_ptr<Buffer> buffer(
        allocator_->Allocate(kInitialPageReaderBuffer));
    if (buffer->data() == NULL) {
      THROW(new Exception(
            ERROR_MEMORY_EXCEEDED,
            "Can not allocate enough memory for PageStreamReader buffer."));
    }

    FailureOrOwned<File> file = OpenFileForReading(name);
        PROPAGATE_ON_FAILURE(file);

    std::unique_ptr<FileRandomPageReader>
        page_reader(new FileRandomPageReader(file.release(),
                                             std::move(buffer),
                                             allocator_));
    PROPAGATE_ON_FAILURE(page_reader->Init());
    return Success(page_reader.release());
  }

  // Opens file with given name, in storage, for reading.
  FailureOrOwned<File> OpenFileForReading(const std::string& stream_name) {
    return OpenFileWithMode<FileT>(stream_name, "r");
  }

  BufferAllocator* allocator_;
  std::unique_ptr<FileSeries> file_name_generator_;
  std::string next_name_;
  const std::string path_;

  DISALLOW_COPY_AND_ASSIGN(ReadableFileStorage);
};


// Creates the ReadableStorage which reads data from files. `FileT` and
// `PathUtilT` should be supersonic::File and supersonic::PathUtil
// implementation. Meaning of `path` depends on chosen implementation.
template<class FileT, class PathUtilT>
FailureOrOwned<ReadableStorage>
    CreateReadableFileStorage(std::unique_ptr<FileSeries> file_series,
                              BufferAllocator* allocator) {
  return Success(new ReadableFileStorage<FileT>(std::move(file_series),
                                                allocator));
}


// Creates the WritableStorage which stores data in files. `FileT` and
// `PathUtilT` should be supersonic::File and supersonic::PathUtil
// implementation. Meaning of `path` depends on chosen implementation.
// TODO(wzoltak): fix comment.
template<class FileT, class PathUtilT>
FailureOrOwned<WritableStorage>
    CreateWritableFileStorage(std::unique_ptr<FileSeries> file_series,
                              BufferAllocator* buffer_allocator) {
  return Success(new WritableFileStorage<FileT>(std::move(file_series),
                                                buffer_allocator));
}

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_CORE_FILE_STORAGE_H_
