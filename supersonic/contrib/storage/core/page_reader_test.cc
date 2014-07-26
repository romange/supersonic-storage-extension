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

#include "supersonic/contrib/storage/core/page_reader.h"

#include <memory>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "supersonic/contrib/storage/base/column_reader.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/contrib/storage/base/page.h"
#include "supersonic/contrib/storage/core/page_builder.h"
#include "supersonic/cursor/infrastructure/table.h"

namespace supersonic {

typedef std::vector<std::unique_ptr<ColumnReader> > ColumnReaderVector;

std::unique_ptr<Cursor>
    PageReader(TupleSchema schema,
               std::shared_ptr<RandomPageReader> page_stream,
               std::unique_ptr<ColumnReaderVector> column_readers,
               uint32_t page_family);

namespace {

class MockRandomPageReader : public RandomPageReader {
 public:
  MOCK_METHOD2(GetPage, FailureOr<const Page*>(uint32_t, uint64_t));
  MOCK_METHOD1(TotalPages, FailureOr<uint64_t>(uint32_t));

  MockRandomPageReader* ExpectingGetPage(int number,
                                         const Page* returned_page) {
    EXPECT_CALL(*this, GetPage(0, number))
        .InSequence(seq)
        .WillOnce(::testing::Return(Success(returned_page)));
    return this;
  }

  MockRandomPageReader* WithTotalPages(uint64_t returned_total) {
    EXPECT_CALL(*this, TotalPages(0))
        .WillRepeatedly(::testing::Return(Success(returned_total)));
    return this;
  }

 private:
  ::testing::Sequence seq;
};

class MockColumnReader : public ColumnReader {
 public:
  explicit MockColumnReader(int uses_streams) : ColumnReader(uses_streams) {}

  MOCK_METHOD1(ReadColumn, FailureOr<const View*>(const Page&));

  MockColumnReader* ExpectingReadColumn(const View* returned_view) {
    EXPECT_CALL(*this, ReadColumn(::testing::_))
        .WillOnce(::testing::Return(Success(returned_view)));
    return this;
  }
};


class PageReaderTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    schema1.add_attribute(Attribute("A", INT32, NULLABLE));
    schema2.add_attribute(Attribute("B", DOUBLE, NOT_NULLABLE));
    mock_page_reader.reset(new MockRandomPageReader());
    mock_column_reader1.reset(new MockColumnReader(2));
    mock_column_reader2.reset(new MockColumnReader(1));
  }

  std::unique_ptr<Page> CreateFilledPage(int streams) {
    PageBuilder page_builder(streams, HeapBufferAllocator::Get());
    FailureOrOwned<Page> page_result = page_builder.CreatePage();
    page_result.mark_checked();
    return std::unique_ptr<Page>(page_result.release());
  }

  std::unique_ptr<Cursor> CreateCursorUnderTest() {
    std::unique_ptr<ColumnReaderVector>
        column_readers(new ColumnReaderVector());
    column_readers->push_back(std::move(mock_column_reader1));
    column_readers->push_back(std::move(mock_column_reader2));
    return PageReader(TupleSchema::Merge(schema1, schema2),
                      mock_page_reader,
                      std::move(column_readers),
                      0);
  }

  TupleSchema schema1;
  TupleSchema schema2;

  std::shared_ptr<MockRandomPageReader> mock_page_reader;
  std::unique_ptr<MockColumnReader> mock_column_reader1;
  std::unique_ptr<MockColumnReader> mock_column_reader2;
};


TEST_F(PageReaderTest, NormalFlow) {
  Table table1(schema1, HeapBufferAllocator::Get());
  TableRowWriter(&table1)
      .AddRow().Int32(15)
      .AddRow().Int32(1).CheckSuccess();

  Table table2(schema2, HeapBufferAllocator::Get());
  TableRowWriter(&table2)
      .AddRow().Double(1.2)
      .AddRow().Double(1.5).CheckSuccess();

  std::unique_ptr<Page> filled_page = CreateFilledPage(3);
  mock_page_reader
      ->WithTotalPages(1)
      ->ExpectingGetPage(0, filled_page.get());
  mock_column_reader1->ExpectingReadColumn(&table1.view());
  mock_column_reader2->ExpectingReadColumn(&table2.view());

  std::unique_ptr<Cursor> page_reader_result = CreateCursorUnderTest();

  for (int i = 0; i < 2; i++) {
    ResultView result = page_reader_result->Next(1);
    ASSERT_TRUE(result.has_data());
    ASSERT_EQ(result.view().row_count(), 1);
    ASSERT_EQ(result.view().column(0).typed_data<INT32>()[0],
              table1.view().column(0).typed_data<INT32>()[i]);
    ASSERT_EQ(result.view().column(1).typed_data<DOUBLE>()[0],
              table2.view().column(0).typed_data<DOUBLE>()[i]);
  }

  ASSERT_TRUE(page_reader_result->Next(1).is_eos());
}

TEST_F(PageReaderTest, EosFinalizesStream) {
  mock_page_reader->WithTotalPages(0);
  std::unique_ptr<Cursor> page_reader_result = CreateCursorUnderTest();
  ASSERT_TRUE(page_reader_result->Next(50).is_eos());
}

TEST_F(PageReaderTest, InconsistentInputThrows) {
  Table table1(schema1, HeapBufferAllocator::Get());
  TableRowWriter(&table1)
      .AddRow().Int32(1).CheckSuccess();

  Table table2(schema2, HeapBufferAllocator::Get());
  TableRowWriter(&table2)
      .AddRow().Double(1.2)
      .AddRow().Double(1.5).CheckSuccess();

  std::unique_ptr<Page> filled_page = CreateFilledPage(3);
  mock_page_reader->WithTotalPages(1)->ExpectingGetPage(0, filled_page.get());
  mock_column_reader1->ExpectingReadColumn(&table1.view());
  mock_column_reader2->ExpectingReadColumn(&table2.view());

  std::unique_ptr<Cursor> page_reader_result = CreateCursorUnderTest();
  ASSERT_TRUE(page_reader_result->Next(50).is_failure());
}

}  // namespace

}  // namespace supersonic
