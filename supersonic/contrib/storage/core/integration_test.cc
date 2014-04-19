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

#include "gtest/gtest.h"

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/infrastructure/writer.h"
#include "supersonic/contrib/storage/base/storage.h"
#include "supersonic/contrib/storage/core/file_storage.h"
#include "supersonic/contrib/storage/core/test_data.h"
#include "supersonic/contrib/storage/core/storage_scan.h"
#include "supersonic/contrib/storage/core/storage_sink.h"
#include "supersonic/contrib/storage/util/path_util.h"
#include "supersonic/utils/file.h"

namespace supersonic {
namespace {

class IntegrationTest : public ::testing::Test {
 protected:
  void SetUp() {
    testing::internal::FilePath directory("/tmp/");
    testing::internal::FilePath basename("supersonic_file_storage");
    // NOTE: Not thread-safe.
    storage_path_ = testing::internal::FilePath::GenerateUniqueFileName(
        directory, basename, "test-storage").ToString();

    schema_ = CreateSchema();
    pieces_.reset(new std::vector<StringPiece>());
    PopulatePieces(pieces_);
  }

  void TearDown() {
    if (PathUtil::ExistsDir(storage_path_)) {
      ASSERT_TRUE(PathUtil::RecursiveDelete(storage_path_));
    }
  }

  std::string storage_path_;
  TupleSchema schema_;
  std::shared_ptr<std::vector<StringPiece> > pieces_;

 private:
  TupleSchema CreateSchema() {
    TupleSchema tuple_schema;
    tuple_schema.add_attribute(Attribute("id", INT32, NULLABLE));
    tuple_schema.add_attribute(Attribute("salary", UINT64, NOT_NULLABLE));
    tuple_schema.add_attribute(Attribute("magic index", DOUBLE, NOT_NULLABLE));
    tuple_schema.add_attribute(Attribute("nickname", STRING, NULLABLE));
    tuple_schema.add_attribute(Attribute("birthday", DATE, NOT_NULLABLE));
    tuple_schema.add_attribute(Attribute("last login", DATETIME, NULLABLE));
    tuple_schema.add_attribute(Attribute("secret key", BINARY, NOT_NULLABLE));
    return tuple_schema;
  }

  void PopulatePieces(std::shared_ptr<std::vector<StringPiece> > pieces) {
    pieces->push_back(StringPiece("ala ma kota", 12));
    pieces->push_back(StringPiece("lorem ipsum dolor imet", 22));
    pieces->push_back(StringPiece("?!@#$%^&*() zxcvbnm", 19));
  }
};

TEST_F(IntegrationTest, FullFlow) {
  BufferAllocator* allocator = HeapBufferAllocator::Get();

  FailureOrOwned<WritableStorage> writable_storage_result =
      CreateWritableFileStorage<File, PathUtil>(storage_path_, allocator);
  ASSERT_TRUE(writable_storage_result.is_success());
  std::unique_ptr<WritableStorage>
      writable_storage(writable_storage_result.release());

  FailureOrOwned<Sink> storage_sink_result =
      CreateStorageSink(schema_, std::move(writable_storage), allocator);
  ASSERT_TRUE(storage_sink_result.is_success());
  std::unique_ptr<Sink> storage_sink(storage_sink_result.release());

  int seeds[] = { 124, -543, 8656, -74512, 23412, 13412, 412 };
  Generator generator(schema_, seeds, pieces_);
  for (int i = 0; i < 3; i++) {
    const View& view = generator.Generate(10000);
    ASSERT_TRUE(storage_sink->Write(view).is_success());
  }
  storage_sink->Finalize();

  FailureOrOwned<ReadableStorage> readable_storage_result =
      CreateReadableFileStorage<File, PathUtil>(storage_path_, allocator);
  ASSERT_TRUE(readable_storage_result.is_success());
  std::unique_ptr<ReadableStorage>
      readable_storage(readable_storage_result.release());

  FailureOrOwned<Cursor> storage_scan_result =
      StorageScan(std::move(readable_storage), allocator);
  ASSERT_TRUE(storage_scan_result.is_success());
  std::unique_ptr<Cursor> storage_scan(storage_scan_result.release());

  std::unique_ptr<Validator> validator = generator.CreateValidator();
  while (true) {
    ResultView result_view = storage_scan->Next(20000);
    if (result_view.is_eos()) {
      break;
    }

    ASSERT_TRUE(result_view.has_data());
    validator->Validate(result_view.view());
  }
}

}  // namespace
}  // namespace supersonic