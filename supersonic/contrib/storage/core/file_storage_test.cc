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
#include "supersonic/contrib/storage/base/storage.h"
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
    ASSERT_TRUE(
            PathUtil::MkDir(storage_path_, S_IRWXU, true /* with parents */));
  }

  void TearDown() {
    if (PathUtil::ExistsDir(storage_path_)) {
      ASSERT_TRUE(PathUtil::RecursiveDelete(storage_path_));
    }
  }

  void AssertEqualPages(const Page& a, const Page& b) {
    ASSERT_EQ(a.PageHeader().total_size, b.PageHeader().total_size);
    ASSERT_EQ(0, memcmp(a.RawData(), b.RawData(), a.PageHeader().total_size));
  }

  void CreateTestPage(std::unique_ptr<const Page>* page) {
    static const char data[] =
        "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do";
    PageBuilder page_builder(3, HeapBufferAllocator::Get());
    ASSERT_TRUE(page_builder.AppendToByteBuffer(0, data, 20).is_success());
    ASSERT_TRUE(page_builder.AppendToByteBuffer(1, data + 5, 17).is_success());
    ASSERT_TRUE(page_builder.AppendToByteBuffer(2, data + 9, 6).is_success());

    FailureOrOwned<Page> page_result = page_builder.CreatePage();
    ASSERT_TRUE(page_result.is_success());
    page->reset(page_result.release());
  }

  std::string storage_path_;
};


TEST_F(FileStorageTest, WriteThenRead) {
  std::unique_ptr<const Page> page;
  CreateTestPage(&page);
  const std::string file_path = File::JoinPath(storage_path_,
                                               "read_then_write");
  const int files = 3;
  const int writes = 2;

  // Write page multiple times.
  std::unique_ptr<FileSeries> output_series = EnumeratedFileSeries(file_path);
  FailureOrOwned<WritableStorage> writable_storage_result =
      CreateWritableFileStorage<File, PathUtil>(std::move(output_series),
                                                HeapBufferAllocator::Get());
  ASSERT_TRUE(writable_storage_result.is_success());
  std::unique_ptr<WritableStorage>
      writable_storage(writable_storage_result.release());

  for (int file = 0; file < files; file++) {
    FailureOrOwned<PageStreamWriter> page_writer_result =
        writable_storage->NextPageStreamWriter();
    ASSERT_TRUE(page_writer_result.is_success());
    std::unique_ptr<PageStreamWriter> page_writer(page_writer_result.release());

    for (int write = 0; write < writes; write++) {
      ASSERT_TRUE(page_writer->AppendPage(*page).is_success());
    }
    page_writer->Finalize();
  }

  // Check for files existence.
  std::unique_ptr<FileSeries> check_series = EnumeratedFileSeries(file_path);
  for (int file = 0; file < files; file++) {
    ASSERT_TRUE(File::Exists(check_series->NextFileName()));
  }

  // Read contents.
  std::unique_ptr<FileSeries> input_series = EnumeratedFileSeries(file_path);
  FailureOrOwned<ReadableStorage> readable_storage_result =
      CreateReadableFileStorage<File, PathUtil>(std::move(input_series),
                                                HeapBufferAllocator::Get());
  ASSERT_TRUE(readable_storage_result.is_success());
  std::unique_ptr<ReadableStorage>
      readable_storage(readable_storage_result.release());

  for (int file = 0; file < files; file++) {
    ASSERT_TRUE(readable_storage->HasNext());

    FailureOrOwned<RandomPageReader> page_reader_result =
        readable_storage->NextRandomPageReader();
    ASSERT_TRUE(page_reader_result.is_success());
    std::unique_ptr<RandomPageReader> page_reader(page_reader_result.release());

    for (int write = 0; write < writes; write++) {
      FailureOr<const Page*> read_page =
          page_reader->GetPage(page->PageHeader().total_size * write);
      ASSERT_TRUE(read_page.is_success());
      AssertEqualPages(*read_page.get(), *page);
    }
    page_reader->Finalize();
  }
  ASSERT_FALSE(readable_storage->HasNext());
}

}  // namespace
}  // namespace supersonic
