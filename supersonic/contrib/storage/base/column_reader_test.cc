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

#include "supersonic/contrib/storage/base/column_reader.h"

#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/contrib/storage/base/deserializer.h"
#include "supersonic/contrib/storage/base/page.h"

namespace supersonic {

std::unique_ptr<ColumnReader> CreateColumnReader(
    int starting_from_stream,
    std::unique_ptr<View> view,
    std::unique_ptr<Deserializer> data_deserializer,
    std::unique_ptr<Deserializer> is_null_deserializer);

namespace {

class MockDeserializer : public Deserializer {
 public:
  // Macros does not like colons and the MOCK_METHOD1 can't take type argument
  // in parentheses.
  typedef std::pair<VariantConstPointer, rowcount_t> ResultPair;

  MOCK_METHOD2(Deserialize, FailureOr<ResultPair>(const void*,
                                                  const ByteBufferHeader&));

  MockDeserializer* ExpectDeserialize(VariantConstPointer ptr_result,
                                      rowcount_t count_result) {
    EXPECT_CALL(*this, Deserialize(::testing::_, ::testing::_))
          .WillOnce(::testing::Return(
              Success(std::make_pair(ptr_result, count_result))));
    return this;
  }
};

// TODO(wzoltak): lacks test whether good buffer was called;

TEST(ColumnReaderTest, TestNormalFlow) {
  TupleSchema schema;
  schema.add_attribute(Attribute("a", INT32, NULLABLE));

  int32_t ints[] = {10, 0, 18, 153};
  bool is_null[] = {false, true, false, false};
  rowcount_t count = 4;

  std::unique_ptr<View> view(new View(schema));
  std::unique_ptr<Deserializer> data_deserializer(
      (new MockDeserializer())
          ->ExpectDeserialize(VariantConstPointer(ints), count));
  std::unique_ptr<Deserializer> is_null_deserializer(
      (new MockDeserializer())
          ->ExpectDeserialize(VariantConstPointer(is_null), count));

  std::unique_ptr<ColumnReader> column_reader =
      CreateColumnReader(0, std::move(view), std::move(data_deserializer),
          std::move(is_null_deserializer));

  PageBuilder page_builder(2, HeapBufferAllocator::Get());
  FailureOrOwned<Page> page_result = page_builder.CreatePage();
  ASSERT_TRUE(page_result.is_success());
  std::unique_ptr<Page> page(page_result.release());

  FailureOr<const View*> result = column_reader->ReadColumn(*page);
  ASSERT_TRUE(result.is_success());
  ASSERT_EQ(count, result.get()->row_count());

  const int32_t* read_ints = result.get()->column(0).typed_data<INT32>();
  ASSERT_EQ(0, memcmp(ints, read_ints, sizeof(int32_t) * count));
  const bool* read_is_null = result.get()->column(0).is_null();
  ASSERT_EQ(0, memcmp(is_null, read_is_null, sizeof(bool) * count));
}

}  // namespace

}  // namespace supersonic
