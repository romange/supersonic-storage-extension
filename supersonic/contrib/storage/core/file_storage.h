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
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "supersonic/base/exception/result.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/contrib/storage/base/byte_stream_writer.h"
#include "supersonic/contrib/storage/base/page.h"
#include "supersonic/contrib/storage/base/page_stream_writer.h"
#include "supersonic/contrib/storage/base/random_page_reader.h"
#include "supersonic/contrib/storage/base/raw_storage.h"
#include "supersonic/contrib/storage/core/page_builder.h"
#include "supersonic/contrib/storage/core/file_series.h"
#include "supersonic/utils/file.h"
#include "supersonic/utils/stringprintf.h"


namespace supersonic {

const size_t kInitialPageReaderBuffer = 1024 * 1024;  // 1MB

const uint64_t kMagicStorageConstant = 1234565746523432454;

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
  explicit FileByteStreamWriter(File* file)
      : file_(file),
        finalized_(false),
        stream_path_(file_->CreateFileName()) {
  }

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
        written_bytes_(0),
        written_pages_(0) {
  }

  virtual FailureOr<uint64_t> AppendPage(uint32_t family, const Page& page) {
    FailureOrVoid appended =
        byte_stream_.AppendBytes(page.RawData(), page.PageHeader().total_size);
    PROPAGATE_ON_FAILURE(appended);

    // Note that [] operator creates empty (default constructor) value.
    uint64_t page_number = page_index_[family].size();
    page_index_[family].emplace_back(written_bytes_);
    written_bytes_ += page.PageHeader().total_size;

    written_pages_++;
    return Success(page_number);
  }

  virtual FailureOrVoid Finalize() {
    PROPAGATE_ON_FAILURE(WriteIndex());
    PROPAGATE_ON_FAILURE(byte_stream_.Finalize());
    return Success();
  }

  virtual size_t WrittenBytes() {
    return written_bytes_;
  }

 private:
  // TODO(wzoltak): Use protobuf instead of manual de(serialization)?
  FailureOrVoid WriteIndex() {
    uint64_t index_offset = written_bytes_;

    // Number of families + family number and number of pages + pages
    // themselves + index_offset + magic integrity seal.
    uint64_t required_size = sizeof(uint32_t)
        + page_index_.size() * (sizeof(uint32_t) + sizeof(uint64_t))
        + written_pages_ * sizeof(uint64_t) + sizeof(uint64_t)
        + sizeof(uint64_t);

    // TODO(wzoltak): use allocator?
    std::unique_ptr<uint8_t[]> buffer(new uint8_t[required_size]);

    google::protobuf::io::ArrayOutputStream
        array_stream(buffer.get(), required_size);
    google::protobuf::io::CodedOutputStream stream(&array_stream);

    stream.WriteLittleEndian32(page_index_.size());
    for (auto& it : page_index_) {
      uint32_t family = it.first;
      uint64_t family_size = it.second.size();

      stream.WriteLittleEndian32(family);
      stream.WriteLittleEndian64(family_size);
      for (uint64_t page_offset : it.second) {
        stream.WriteLittleEndian64(page_offset);
      }
    }

    stream.WriteLittleEndian64(index_offset);
    stream.WriteLittleEndian64(kMagicStorageConstant);

    PROPAGATE_ON_FAILURE(
        byte_stream_.AppendBytes(buffer.get(), stream.ByteCount()));

    return Success();
  }

  std::map<uint32_t, std::vector<uint64_t>> page_index_;
  FileByteStreamWriter byte_stream_;
  uint64_t written_bytes_;
  uint64_t written_pages_;
  DISALLOW_COPY_AND_ASSIGN(FilePageStreamWriter);
};

// WritableStorage which stores data in files, operating via the
// supersonic::File interface.
template<class FileT>
class WritableFileStorageImplementation : public WritableRawStorage {
 public:
  WritableFileStorageImplementation(std::unique_ptr<FileSeries> file_series,
                                    BufferAllocator* allocator)
      : file_series_(std::move(file_series)) {}

  virtual ~WritableFileStorageImplementation() {}

  FailureOrOwned<PageStreamWriter> NextPageStreamWriter() {
    if (!file_series_->HasNext()) {
      THROW(new Exception(ERROR_INVALID_STATE,
                          "Unable to obtain next file name in series"));
    }
    return CreatePageStreamWriter(file_series_->Next());
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

  DISALLOW_COPY_AND_ASSIGN(WritableFileStorageImplementation);
};

class FileRandomPageReader : public RandomPageReader {
 public:
  FileRandomPageReader(File* file, std::unique_ptr<Buffer> buffer,
                       BufferAllocator* allocator)
      : file_(file),
        buffer_(std::move(buffer)),
        allocator_(allocator),
        file_size_(0) {}

  virtual ~FileRandomPageReader() {
    if (!file_->Close()) {
      LOG(ERROR) << "Can not close the underlying file.";
    }
  }

  // Inits the instances. Must be called before any other method.
  FailureOrVoid Init() {
    file_size_ = file_->Size();
    if (file_size_ < 0) {
      THROW(new Exception(
          ERROR_GENERAL_IO_ERROR,
          "Unable to retrieve total size of underlying file."));
    }

    PROPAGATE_ON_FAILURE(ReadIndex());
    return Success();
  }

  FailureOr<const Page*> GetPage(uint32_t family, uint64_t number) {
    uint64_t page_offset = page_index_[family][number];
    uint64_t page_size;
    PROPAGATE_ON_FAILURE(
        ReadExactly(page_offset, &page_size, sizeof(uint64_t)));
    MaybeResizeBuffer(page_size);

    PROPAGATE_ON_FAILURE(
        ReadExactly(page_offset, buffer_->data(), page_size));

    FailureOrOwned<Page> page_result = CreatePageView(*buffer_.get());
    PROPAGATE_ON_FAILURE(page_result);
    read_page_.reset(page_result.release());

    return Success(read_page_.get());
  }

  virtual FailureOr<uint64_t> TotalPages(uint32_t family) {
    auto it = page_index_.find(family);
    if (it == page_index_.end()) {
      THROW(new Exception(ERROR_INVALID_ARGUMENT_VALUE,
                          StringPrintf("Unknown page family '%d'.", family)));
    }
    return Success(it->second.size());
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
      THROW(new Exception(ERROR_GENERAL_IO_ERROR,
                          "Unable to seek through underlying file."));
    }

    uint8_t* byte_buffer = static_cast<uint8_t*>(buffer);
    do {
      int64 read_bytes = file_->Read(byte_buffer, length);
      if (read_bytes < 0) {
        THROW(new Exception(ERROR_GENERAL_IO_ERROR,
                            "Error during read from underlying file"));
      } else if (read_bytes == 0) {
        THROW(new Exception(ERROR_GENERAL_IO_ERROR, "Unexpected end of file"));
      }
      byte_buffer += read_bytes;
      length -= read_bytes;
    } while (length > 0);

    return Success();
  }

  FailureOrVoid ReadIndex() {
    uint64_t tail_size = 2 * sizeof(uint64_t);
    std::unique_ptr<uint8_t[]> file_tail(new uint8_t[tail_size]);
    PROPAGATE_ON_FAILURE(
        ReadExactly(file_size_ - tail_size, file_tail.get(), tail_size));

    // Read tail with index offset and integrity seal.
    uint64_t index_offset;
    uint64_t integrity_seal;
    google::protobuf::io::CodedInputStream
        tail_stream(file_tail.get(), tail_size);
    tail_stream.ReadLittleEndian64(&index_offset);
    tail_stream.ReadLittleEndian64(&integrity_seal);

    PROPAGATE_ON_FAILURE(CheckIntegritySeal(integrity_seal));

    // Read contents of serialized index.
    uint64_t index_size = file_size_ - tail_size - index_offset;
    PROPAGATE_ON_FAILURE(MaybeResizeBuffer(index_size));
    PROPAGATE_ON_FAILURE(
        ReadExactly(index_offset, buffer_->data(), index_size));

    google::protobuf::io::CodedInputStream
        stream(static_cast<uint8_t*>(buffer_->data()), index_size);

    // Deserialize index.
    uint32_t families_count;
    stream.ReadLittleEndian32(&families_count);
    for (int family_index = 0; family_index < families_count; family_index++) {
      uint32_t family;
      stream.ReadLittleEndian32(&family);
      uint64_t family_size;
      stream.ReadLittleEndian64(&family_size);

      std::vector<uint64_t>& pages_offsets = page_index_[family];
      for (int page_index = 0; page_index < family_size; page_index++) {
        uint64_t page_offset;
        stream.ReadLittleEndian64(&page_offset);
        pages_offsets.emplace_back(page_offset);
      }
    }
    return Success();
  }

  FailureOrVoid CheckIntegritySeal(uint64_t seal) {
    if (seal != kMagicStorageConstant) {
      THROW(new Exception(
          ERROR_GENERAL_IO_ERROR,
          StringPrintf("Storage file corrupted! Expected %lu got %lu.",
                       kMagicStorageConstant, seal)));
    }
    return Success();
  }

  std::map<uint32_t, std::vector<uint64_t>> page_index_;
  File* file_;
  std::unique_ptr<Buffer> buffer_;
  BufferAllocator* allocator_;
  std::unique_ptr<Page> read_page_;
  int64_t file_size_;
  DISALLOW_COPY_AND_ASSIGN(FileRandomPageReader);
};


// ReadableStorage which reads data from files, operating via the
// supersonic::File interface.
template<class FileT>
class ReadableFileStorageImplementation : public ReadableRawStorage {
 public:
  explicit ReadableFileStorageImplementation(
      std::unique_ptr<FileSeries> file_series,
      BufferAllocator* allocator)
          : allocator_(allocator),
            file_name_generator_(std::move(file_series)) {}

  FailureOrOwned<RandomPageReader> NextRandomPageReader() {
    FailureOrOwned<RandomPageReader> result =
        CreateRandomPageReader(file_name_generator_->Next());
    PROPAGATE_ON_FAILURE(result);
    return result;
  }

  bool HasNext() {
    return file_name_generator_->HasNext() &&
        FileT::Exists(FileT::JoinPath(path_, file_name_generator_->PeepNext()));
  }

 private:
  virtual FailureOrOwned<RandomPageReader> CreateRandomPageReader(
      const std::string& name) {
    std::unique_ptr<Buffer>
        buffer(allocator_->Allocate(kInitialPageReaderBuffer));
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
    FailureOrVoid init_result = page_reader->Init();
    PROPAGATE_ON_FAILURE(init_result);
    return Success(page_reader.release());
  }

  // Opens file with given name, in storage, for reading.
  FailureOrOwned<File> OpenFileForReading(const std::string& stream_name) {
    return OpenFileWithMode<FileT>(stream_name, "r");
  }

  BufferAllocator* allocator_;
  std::unique_ptr<FileSeries> file_name_generator_;
  const std::string path_;

  DISALLOW_COPY_AND_ASSIGN(ReadableFileStorageImplementation);
};

// Creates the ReadableStorage which reads data from files. `FileT` and
// `PathUtilT` should be supersonic::File and supersonic::PathUtil
// implementation. Meaning of `path` depends on chosen implementation.
template<class FileT>
FailureOrOwned<ReadableRawStorage>
    ReadableFileStorage(std::unique_ptr<FileSeries> file_series,
                        BufferAllocator* allocator) {
  return Success(
      new ReadableFileStorageImplementation<FileT>(std::move(file_series),
                                                   allocator));
}

// Creates the WritableStorage which stores data in files. `FileT` and
// `PathUtilT` should be supersonic::File and supersonic::PathUtil
// implementation. Meaning of `path` depends on chosen implementation.
// TODO(wzoltak): fix comment.
template<class FileT>
FailureOrOwned<WritableRawStorage>
    WritableFileStorage(std::unique_ptr<FileSeries> file_series,
                        BufferAllocator* buffer_allocator) {
  return Success(
      new WritableFileStorageImplementation<FileT>(std::move(file_series),
                                                   buffer_allocator));
}

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_CORE_FILE_STORAGE_H_
