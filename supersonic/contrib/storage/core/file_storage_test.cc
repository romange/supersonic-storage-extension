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

#include "supersonic/contrib/storage/core/file_storage.h"

#include <stdio.h>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "supersonic/base/exception/result.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/contrib/storage/core/page_builder.h"
#include "supersonic/contrib/storage/util/path_util.h"
#include "supersonic/utils/file.h"


namespace supersonic {
namespace {

class FileStorageTest : public ::testing::Test {
 protected:
  void SetUp() {
    testing::internal::FilePath directory("/tmp/");
    testing::internal::FilePath basename("supersonic_file_storage");
    // NOTE: Not thread-safe.
    storage_path_ = testing::internal::FilePath::GenerateUniqueFileName(
        directory, basename, "test").ToString();
  }

  void TearDown() {
    if (PathUtil::ExistsDir(storage_path_)) {
      ASSERT_TRUE(PathUtil::RecursiveDelete(storage_path_));
    }
  }

  std::unique_ptr<const Page> CreateTestPage() {
    PageBuilder page_builder(0, HeapBufferAllocator::Get());
    FailureOrOwned<Page> page_result = page_builder.CreatePage();
    page_result.mark_checked();
    return std::unique_ptr<const Page>(page_result.release());
  }

  void CreateStorage(const std::string& path) {
    FailureOrOwned<Storage> storage =
        CreateFileStorage<File, PathUtil>(storage_path_);
    ASSERT_TRUE(storage.is_success());
    storage_ = std::unique_ptr<Storage>(storage.release());
  }

  void CreateStreamWriter(const std::string& stream_name) {
    FailureOrOwned<PageStreamWriter> stream_writer =
          storage_->CreatePageStream(stream_name);
    ASSERT_TRUE(stream_writer.is_success());
    stream_writer_ = std::unique_ptr<PageStreamWriter>(stream_writer.release());
  }

  void GetFileContents(char* buffer, const std::string& path) {
    File* file = File::OpenOrDie(path, "r");
    ASSERT_TRUE(file != NULL);
    ASSERT_GT(file->Read(buffer, 1024), 0);
    ASSERT_TRUE(file->eof());
    ASSERT_TRUE(file->Close());
  }

  std::string storage_path_;
  std::unique_ptr<Storage> storage_;
  std::unique_ptr<PageStreamWriter> stream_writer_;
};

TEST_F(FileStorageTest, CreateFileStorageCreatesDirectory) {
  ASSERT_FALSE(PathUtil::ExistsDir(storage_path_));
  CreateStorage(storage_path_);
  ASSERT_TRUE(PathUtil::ExistsDir(storage_path_));
}

TEST_F(FileStorageTest, CreatingStreamCreatesFile) {
  const std::string stream_name = "test_stream";

  CreateStorage(storage_path_);
  CreateStreamWriter(stream_name);
  ASSERT_TRUE(File::Exists(File::JoinPath(storage_path_, stream_name)));
  stream_writer_->Finalize();
}

TEST_F(FileStorageTest, WritingToStorageWritesToFile) {
  const std::string stream_name = "test_stream";
  std::unique_ptr<char> buffer(new char[128]);

  CreateStorage(storage_path_);
  CreateStreamWriter(stream_name);
  std::unique_ptr<const Page> page = CreateTestPage();
  ASSERT_TRUE(stream_writer_->AppendPage(*page).is_success());
  stream_writer_->Finalize();

  GetFileContents(buffer.get(), File::JoinPath(storage_path_, stream_name));
  ASSERT_EQ(
      memcmp(buffer.get(), page->RawData(), page->PageHeader().total_size),
      0);
}

TEST_F(FileStorageTest, WritingToFinalizedThrows) {
  const std::string stream_name = "test_stream";

  CreateStorage(storage_path_);
  CreateStreamWriter(stream_name);
  stream_writer_->Finalize();
  std::unique_ptr<const Page> page = CreateTestPage();
  ASSERT_TRUE(stream_writer_->AppendPage(*page).is_failure());
}

}  // namespace
}  // namespace supersonic
