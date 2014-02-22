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
    if (streams_.find(name) != streams_.end()) {
      THROW(new Exception(
          ERROR_INVALID_ARGUMENT_VALUE,
          StringPrintf("Stream %s already exists.", name.c_str())));
    }

    const std::string stream_path = FileT::JoinPath(path_, name);
    File* file = FileT::OpenOrDie(stream_path, "w+");
    if (file == nullptr) {
      THROW(new Exception(ERROR_GENERAL_IO_ERROR, strerror(errno)));
    }
    return Success(new FilePageStreamWriter(file));
  }

 private:
  const std::string path_;
  std::set<std::string> streams_;

  // Internal PageStreamWriter implementation for FileStorage.
  class FilePageStreamWriter : public PageStreamWriter {
   public:
    explicit FilePageStreamWriter(File* file) : file_(file), finalized_(false),
        stream_path_(file_->CreateFileName()) {}

    ~FilePageStreamWriter() {
      if (!finalized_) {
        LOG(DFATAL) << "Destroying not finalized FilePageStream.";
        Finalize();
      }
    }

    virtual FailureOrVoid AppendPage(const Page& page) {
      if (finalized_) {
        THROW(new Exception(
            ERROR_INVALID_STATE,
            StringPrintf("Writing to finalized page stream under '%s'.",
                         stream_path_.c_str())));
      }

      int64 written_bytes = file_->Write(page.RawData(),
                                         page.PageHeader().total_size);
      if (written_bytes != page.PageHeader().total_size) {
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
    File* file_;
    bool finalized_;
    const std::string stream_path_;
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
  }
  return Success(new FileStorage<FileT>(path));
}

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_CORE_FILE_STORAGE_H_
