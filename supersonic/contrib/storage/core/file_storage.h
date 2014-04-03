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
#include <sys/stat.h>
#include <set>
#include <string>

#include "supersonic/base/exception/result.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/contrib/storage/base/byte_stream_writer.h"
#include "supersonic/contrib/storage/base/page_stream_writer.h"
#include "supersonic/contrib/storage/base/storage.h"
#include "supersonic/utils/file.h"
#include "supersonic/utils/stringprintf.h"

namespace supersonic {

// Storage which stores data in files, operating via the supersonic::File
// interface.
template<class FileT>
class FileStorage : public Storage {
 public:
  explicit FileStorage(const std::string& path) : path_(path) {
    static_assert(std::is_base_of<File, FileT>::value,
        "Not a supersonic::File interface implementation!");
  }

  virtual FailureOrOwned<PageStreamWriter> CreatePageStream(
      const std::string& name) {
    FailureOrOwned<File> file = OpenFileForNewStream(name);
    PROPAGATE_ON_FAILURE(file);
    return Success(new FilePageStreamWriter(file.release()));
  }

  virtual FailureOrOwned<ByteStreamWriter> CreateByteStream(
      const std::string& name) {
    FailureOrOwned<File> file = OpenFileForNewStream(name);
    PROPAGATE_ON_FAILURE(file);
    return Success(new FileByteStreamWriter(file.release()));
  }

 private:
  FailureOrOwned<File> OpenFileForNewStream(const std::string& stream_name) {
    if (streams_.find(stream_name) != streams_.end()) {
      THROW(new Exception(
          ERROR_INVALID_ARGUMENT_VALUE,
          StringPrintf("Stream %s already exists.", stream_name.c_str())));
    }

    const std::string stream_path = FileT::JoinPath(path_, stream_name);
    File* file = FileT::OpenOrDie(stream_path, "w+");
    if (file == nullptr) {
      THROW(new Exception(ERROR_GENERAL_IO_ERROR, strerror(errno)));
    }
    return Success(file);
  }

  const std::string path_;
  std::set<std::string> streams_;


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

    virtual ~FilePageStreamWriter() {}

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

  DISALLOW_COPY_AND_ASSIGN(FileStorage);
};

template<class FileT, class PathUtilT>
FailureOrOwned<Storage> CreateFileStorage(const std::string& path) {
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
  return Success(new FileStorage<FileT>(path));
}

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_CORE_FILE_STORAGE_H_
