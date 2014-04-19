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

  void AssertEqualPages(const Page& a, const Page& b) {
    ASSERT_EQ(a.PageHeader().total_size, b.PageHeader().total_size);
    ASSERT_EQ(0, memcmp(a.RawData(), b.RawData(), a.PageHeader().total_size));
  }

  void CreateTestFile(const std::string& name,
                      const void* contents,
                      uint64_t size) {
    File* file = File::OpenOrDie(File::JoinPath(storage_path_, name), "w+");
    ASSERT_NE(nullptr, file);
    if (size > 0) {
      int64_t written = file->Write(contents, size);
      ASSERT_EQ(size, written);
    }
    ASSERT_TRUE(file->Close());
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

  void CreateWritableStorage(const std::string& path) {
    FailureOrOwned<WritableStorage> storage =
        CreateWritableFileStorage<File, PathUtil>(storage_path_,
                                                  HeapBufferAllocator::Get());
    ASSERT_TRUE(storage.is_success());
    writable_storage_ = std::unique_ptr<WritableStorage>(storage.release());
  }

  void CreateReadableStorage(const std::string& path) {
    PathUtil::MkDir(path, S_IRWXU, true /* with parents */);
    FailureOrOwned<ReadableStorage> storage =
        CreateReadableFileStorage<File, PathUtil>(storage_path_,
                                                  HeapBufferAllocator::Get());
    if (storage.is_failure()) {
      printf("%s\n", storage.exception().ToString().c_str());
    }
    ASSERT_TRUE(storage.is_success());
    readable_storage_ = std::unique_ptr<ReadableStorage>(storage.release());
  }

  void CreatePageStreamWriter(const std::string& stream_name) {
    FailureOrOwned<PageStreamWriter> stream_writer =
          writable_storage_->CreatePageStreamWriter(stream_name);
    ASSERT_TRUE(stream_writer.is_success());
    page_stream_writer_.reset(stream_writer.release());
  }

  void CreatePageStreamReader(const std::string& stream_name) {
    FailureOrOwned<PageStreamReader> stream_reader =
        readable_storage_->CreatePageStreamReader(stream_name);
    ASSERT_TRUE(stream_reader.is_success());
    page_stream_reader_.reset(stream_reader.release());
  }

  void CreateByteStreamWriter(const std::string& stream_name) {
    FailureOrOwned<ByteStreamWriter> stream_writer =
          writable_storage_->CreateByteStreamWriter(stream_name);
    ASSERT_TRUE(stream_writer.is_success());
    byte_stream_writer_ = std::unique_ptr<ByteStreamWriter>(
        stream_writer.release());
  }

  void CreateByteStreamReader(const std::string& stream_name) {
    FailureOrOwned<ByteStreamReader> stream_reader =
        readable_storage_->CreateByteStreamReader(stream_name);
    ASSERT_TRUE(stream_reader.is_success());
    byte_stream_reader_ = std::unique_ptr<ByteStreamReader>(
        stream_reader.release());
  }

  void GetFileContents(char* buffer, const std::string& path) {
    File* file = File::OpenOrDie(path, "r");
    ASSERT_TRUE(file != NULL);
    ASSERT_GT(file->Read(buffer, 1024), 0);
    ASSERT_TRUE(file->eof());
    ASSERT_TRUE(file->Close());
  }

  std::string storage_path_;
  std::unique_ptr<WritableStorage> writable_storage_;
  std::unique_ptr<ReadableStorage> readable_storage_;
  std::unique_ptr<PageStreamWriter> page_stream_writer_;
  std::unique_ptr<PageStreamReader> page_stream_reader_;
  std::unique_ptr<ByteStreamWriter> byte_stream_writer_;
  std::unique_ptr<ByteStreamReader> byte_stream_reader_;

  static const std::string page_stream_name;
  static const std::string page_stream_reader_name;
  static const std::string byte_stream_name;
  static const std::string byte_stream_reader_name;
};

const std::string FileStorageTest::page_stream_name = "test_page_stream";
const std::string FileStorageTest::byte_stream_name = "test_byte_stream";
const std::string FileStorageTest::page_stream_reader_name =
    "test_page_stream_reader";
const std::string FileStorageTest::byte_stream_reader_name =
    "test_byte_stream_reader";


TEST_F(FileStorageTest, CreateWritableFileStorageCreatesDirectory) {
  ASSERT_FALSE(PathUtil::ExistsDir(storage_path_));
  CreateWritableStorage(storage_path_);
  ASSERT_TRUE(PathUtil::ExistsDir(storage_path_));
}

TEST_F(FileStorageTest, CreatingStreamCreatesFile) {
  CreateWritableStorage(storage_path_);

  CreatePageStreamWriter(page_stream_name);
  ASSERT_TRUE(File::Exists(File::JoinPath(storage_path_, page_stream_name)));
  page_stream_writer_->Finalize();

  CreateByteStreamWriter(byte_stream_name);
  ASSERT_TRUE(File::Exists(File::JoinPath(storage_path_, byte_stream_name)));
  byte_stream_writer_->Finalize();
}

TEST_F(FileStorageTest, WritingToByteStreamWritesToFile) {
  const std::string stream_name = "test_stream";
  const char test_string[] = "test string";
  std::unique_ptr<char> buffer(new char[128]);

  CreateWritableStorage(storage_path_);
  CreateByteStreamWriter(stream_name);
  ASSERT_TRUE(byte_stream_writer_->AppendBytes(
      test_string, strlen(test_string)).is_success());
  byte_stream_writer_->Finalize();

  GetFileContents(buffer.get(), File::JoinPath(storage_path_, stream_name));
  ASSERT_EQ(memcmp(buffer.get(), test_string, strlen(test_string)), 0);
}

TEST_F(FileStorageTest, WritingToPageStreamWritesToFile) {
  const std::string stream_name = "test_stream";
  std::unique_ptr<char> buffer(new char[128]);

  CreateWritableStorage(storage_path_);
  CreatePageStreamWriter(stream_name);
  std::unique_ptr<const Page> page;
  CreateTestPage(&page);
  ASSERT_TRUE(page_stream_writer_->AppendPage(*page).is_success());
  page_stream_writer_->Finalize();

  GetFileContents(buffer.get(), File::JoinPath(storage_path_, stream_name));
  ASSERT_EQ(
      memcmp(buffer.get(), page->RawData(), page->PageHeader().total_size), 0);
}

TEST_F(FileStorageTest, WritingToFinalizedThrows) {
  CreateWritableStorage(storage_path_);

  CreatePageStreamWriter(page_stream_name);
  page_stream_writer_->Finalize();
  std::unique_ptr<const Page> page;
  CreateTestPage(&page);
  ASSERT_TRUE(page_stream_writer_->AppendPage(*page).is_failure());

  CreateByteStreamWriter(byte_stream_name);
  byte_stream_writer_->Finalize();
  ASSERT_TRUE(byte_stream_writer_->AppendBytes(nullptr, 0).is_failure());
}

TEST_F(FileStorageTest, OpeningOpenedStreamFails) {
  CreateWritableStorage(storage_path_);
  const std::string stream_name("test_stream");

  FailureOrOwned<PageStreamWriter> first_stream_writer =
            writable_storage_->CreatePageStreamWriter(stream_name);
  FailureOrOwned<PageStreamWriter> second_stream_writer =
            writable_storage_->CreatePageStreamWriter(stream_name);
  ASSERT_TRUE(first_stream_writer.is_success());
  ASSERT_TRUE(second_stream_writer.is_failure());

  first_stream_writer->Finalize();
}

TEST_F(FileStorageTest, ReadingFromFinalizedThrows) {
  CreateReadableStorage(storage_path_);
  CreateTestFile(byte_stream_reader_name, nullptr, 0);

  CreateByteStreamReader(byte_stream_reader_name);
  byte_stream_reader_->Finalize();
  ASSERT_TRUE(byte_stream_reader_->ReadBytes(nullptr, 0).is_failure());

  CreateTestFile(page_stream_reader_name, nullptr, 0);
  CreatePageStreamReader(page_stream_reader_name);
  page_stream_reader_->Finalize();
  ASSERT_TRUE(page_stream_reader_->NextPage().is_failure());
}

TEST_F(FileStorageTest, ReadingFromByteStream) {
  const char expected[] = "bacon ipsum";
  char actual[32];
  int64_t length = strlen(expected);

  CreateReadableStorage(storage_path_);
  CreateTestFile(byte_stream_reader_name, expected, length);
  CreateByteStreamReader(byte_stream_reader_name);

  FailureOr<int64_t> read_result =
      byte_stream_reader_->ReadBytes(actual, length);
  ASSERT_TRUE(read_result.is_success());
  ASSERT_EQ(length, read_result.get());
  ASSERT_EQ(0, memcmp(expected, actual, length));

  ASSERT_TRUE(byte_stream_reader_->Finalize().is_success());
}

TEST_F(FileStorageTest, ReadingFromPageStream) {
  std::unique_ptr<const Page> page;
  CreateTestPage(&page);

  uint64_t page_size = page->PageHeader().total_size;
  std::unique_ptr<char> data(new char[page_size * 2]);
  memcpy(data.get(), page->RawData(), page_size);
  memcpy(data.get() + page_size, page->RawData(), page_size);

  CreateReadableStorage(storage_path_);
  CreateTestFile(page_stream_reader_name,
                 data.get(),
                 page_size * 2);
  CreatePageStreamReader(page_stream_reader_name);

  for (int i = 0; i < 2; i++) {
    FailureOr<const Page*> read_page_result = page_stream_reader_->NextPage();
    ASSERT_TRUE(read_page_result.is_success());
    AssertEqualPages(*read_page_result.get(), *page);
  }

  FailureOr<const Page*> empty_page_result = page_stream_reader_->NextPage();
  ASSERT_TRUE(empty_page_result.is_success());
  ASSERT_EQ(0, empty_page_result.get()->PageHeader().byte_buffers_count);

  ASSERT_TRUE(page_stream_reader_->Finalize().is_success());
}


}  // namespace
}  // namespace supersonic
