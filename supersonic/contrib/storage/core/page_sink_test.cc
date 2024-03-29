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

#include "supersonic/contrib/storage/core/page_sink.h"

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "supersonic/base/exception/result.h"
#include "supersonic/contrib/storage/base/column_writer.h"
#include "supersonic/contrib/storage/base/page.h"
#include "supersonic/contrib/storage/base/page_stream_writer.h"
#include "supersonic/contrib/storage/core/data_type_serializer.h"
#include "supersonic/contrib/storage/core/page_builder.h"
#include "supersonic/contrib/storage/core/file_storage.h"
#include "supersonic/contrib/storage/core/storage_sink.h"
#include "supersonic/contrib/storage/util/path_util.h"
#include "supersonic/cursor/infrastructure/table.h"
#include "supersonic/utils/file.h"


namespace supersonic {

FailureOrOwned<PageSink> CreatePageSink(
    std::unique_ptr<const BoundSingleSourceProjector> projector,
    std::shared_ptr<PageStreamWriter> page_stream_writer,
    std::unique_ptr<std::vector<std::unique_ptr<ColumnWriter> > >
        column_writers,
    std::shared_ptr<PageBuilder> page_builder,
    std::shared_ptr<MetadataWriter> metadata_writer,
    uint32_t page_family);

namespace {


MATCHER_P(MatchingColumn, column, "") {
  return arg.data().raw() == column->data().raw();
}

class MockColumnWriter : public ColumnWriter {
 public:
  explicit MockColumnWriter(int uses_streams) : ColumnWriter(uses_streams) {}
  MOCK_METHOD2(WriteColumn, FailureOrVoid(const Column&, rowcount_t));

  MockColumnWriter* ExpectingWriteColumn(const Column* column) {
    EXPECT_CALL(*this, WriteColumn(MatchingColumn(column), ::testing::_))
        .WillOnce(::testing::Return(Success()));
    return this;
  }
};


MATCHER_P(StreamsInPage, num, "") {
  return arg.PageHeader().byte_buffers_count == num;
}


class MockPageStreamWriter : public PageStreamWriter {
 public:
  MOCK_METHOD2(AppendPage, FailureOr<uint64_t>(uint32_t, const Page& page));
  MOCK_METHOD0(Finalize, FailureOrVoid());
  MOCK_METHOD0(WrittenBytes, size_t());

  MockPageStreamWriter* ExpectingAppendPage(uint32_t streams_in_page,
                                            uint64_t page_number) {
    EXPECT_CALL(*this, AppendPage(0, StreamsInPage(streams_in_page)))
        .WillOnce(::testing::Return(Success(page_number)));
    return this;
  }
};

MATCHER_P(PageMetadataRowcount, row_count, "") {
  return arg.row_count() == row_count;
}

class MockMetadataWriter : public MetadataWriter {
 public:
  MOCK_METHOD2(AppendPage, FailureOrVoid(uint32_t, const PageMetadata&));
  MOCK_METHOD0(DumpToPage, FailureOrOwned<Page>());

  MockMetadataWriter* ExpectingAppendPage(uint64_t row_count) {
    EXPECT_CALL(*this, AppendPage(0, PageMetadataRowcount(row_count)))
        .WillOnce(::testing::Return(Success()));
    return this;
  }
};


class PageSinkTest : public ::testing::Test {
 protected:
  std::unique_ptr<const BoundSingleSourceProjector> CreateProjector(
      const TupleSchema schema) {
    std::unique_ptr<BoundSingleSourceProjector>
        projector(new BoundSingleSourceProjector(schema));
    for (int i = 0; i < schema.attribute_count(); i++) {
      projector->Add(i);
    }
    return std::move(projector);
  }

  TupleSchema CreateSchema() {
    TupleSchema schema;
    schema.add_attribute(Attribute("A", BOOL, NOT_NULLABLE));
    schema.add_attribute(Attribute("B", INT32, NULLABLE));
    return schema;
  }

  std::unique_ptr<Table> CreateTableWithData(TupleSchema schema) {
    std::unique_ptr<Table>
        table(new Table(schema, HeapBufferAllocator::Get()));
    TableRowWriter(table.get())
        .AddRow().Bool(true).Int32(5)
        .AddRow().Bool(false).Null()
        .CheckSuccess();
    return std::move(table);
  }
};


TEST_F(PageSinkTest, DataPassedToColumnWriters) {
  TupleSchema schema = CreateSchema();
  std::unique_ptr<const BoundSingleSourceProjector> projector =
      CreateProjector(schema);
  std::unique_ptr<Table> table = CreateTableWithData(schema);
  std::shared_ptr<PageStreamWriter> page_stream(
      (new MockPageStreamWriter())->ExpectingAppendPage(3, 0));
  std::shared_ptr<PageBuilder>
      page_builder(new PageBuilder(3, HeapBufferAllocator::Get()));
  std::shared_ptr<MetadataWriter>
      metadata_writer((new MockMetadataWriter())
          ->ExpectingAppendPage(table->row_count()));

  std::unique_ptr<std::vector<std::unique_ptr<ColumnWriter> > >
      column_writers(new std::vector<std::unique_ptr<ColumnWriter> >());
  column_writers->emplace_back(
      (new MockColumnWriter(1))
          ->ExpectingWriteColumn(&table->view().column(0)));
  column_writers->emplace_back(
      (new MockColumnWriter(2))
          ->ExpectingWriteColumn(&table->view().column(1)));

  FailureOrOwned<PageSink> page_sink_result =
      CreatePageSink(std::move(projector),
                     page_stream,
                     std::move(column_writers),
                     page_builder,
                     metadata_writer,
                     0 /* page family */);
  ASSERT_TRUE(page_sink_result.is_success());
  std::unique_ptr<Sink> page_sink(page_sink_result.release());

  FailureOr<rowcount_t> written_rows = page_sink->Write(table->view());
  ASSERT_TRUE(written_rows.is_success());
  ASSERT_EQ(table->row_count(), written_rows.get());

  ASSERT_TRUE(page_sink->Finalize().is_success());
}


TEST_F(PageSinkTest, FinalizeFinalizesStream) {
  TupleSchema schema;
  std::unique_ptr<const BoundSingleSourceProjector> projector =
      CreateProjector(schema);
  std::shared_ptr<PageStreamWriter> page_stream(new MockPageStreamWriter());
  std::shared_ptr<MetadataWriter> metadata_writer(new MockMetadataWriter());

  FailureOrOwned<PageSink> page_sink_result =
      CreatePageSink(std::move(projector),
                     page_stream,
                     metadata_writer,
                     0 /* page family */,
                     HeapBufferAllocator::Get());
  ASSERT_TRUE(page_sink_result.is_success());
  std::unique_ptr<Sink> page_sink(page_sink_result.release());

  View view(schema);
  ASSERT_TRUE(page_sink->Finalize().is_success());
  ASSERT_TRUE(page_sink->Write(view).is_failure());
}


TEST_F(PageSinkTest, FinalizingDumpsLastPage) {
  TupleSchema schema = CreateSchema();
  std::unique_ptr<Table> table = CreateTableWithData(schema);

  std::unique_ptr<const BoundSingleSourceProjector> projector =
      CreateProjector(schema);
  std::shared_ptr<MockPageStreamWriter> page_stream(
      (new MockPageStreamWriter())->ExpectingAppendPage(3, 0));
  std::shared_ptr<MockMetadataWriter>
      metadata_writer((new MockMetadataWriter())
          ->ExpectingAppendPage(table->row_count()));

  FailureOrOwned<PageSink> page_sink_result =
      CreatePageSink(std::move(projector),
                     page_stream,
                     metadata_writer,
                     0 /* page family */,
                     HeapBufferAllocator::Get());
  ASSERT_TRUE(page_sink_result.is_success());
  std::unique_ptr<Sink> page_sink(page_sink_result.release());

  // Write very small portion of data, so it won't trigger writing to stream.
  FailureOr<rowcount_t> written_rows = page_sink->Write(table->view());
  ASSERT_TRUE(written_rows.is_success());
  ASSERT_EQ(table->row_count(), written_rows.get());

  ASSERT_TRUE(page_sink->Finalize().is_success());
}


TEST_F(PageSinkTest, IsUsingProjector) {
  TupleSchema schema = CreateSchema();
  std::unique_ptr<Table> table = CreateTableWithData(schema);

  std::shared_ptr<MockPageStreamWriter> page_stream(
      (new MockPageStreamWriter())->ExpectingAppendPage(2, 0));
  std::shared_ptr<MetadataWriter>
      metadata_writer((new MockMetadataWriter())
          ->ExpectingAppendPage(table->row_count()));

  NamedAttributeProjector unbound_projector("B");
  FailureOrOwned<const BoundSingleSourceProjector> projector_result =
      unbound_projector.Bind(schema);
  ASSERT_TRUE(projector_result.is_success());
  std::unique_ptr<const BoundSingleSourceProjector>
      projector(projector_result.release());

  FailureOrOwned<PageSink> page_sink_result =
      CreatePageSink(std::move(projector),
                     page_stream,
                     metadata_writer,
                     0 /* page family */,
                     HeapBufferAllocator::Get());
  ASSERT_TRUE(page_sink_result.is_success());
  std::unique_ptr<Sink> page_sink(page_sink_result.release());


  FailureOr<rowcount_t> written_rows = page_sink->Write(table->view());
  ASSERT_TRUE(written_rows.is_success());
  ASSERT_EQ(table->view().row_count(), written_rows.get());

  ASSERT_TRUE(page_sink->Finalize().is_success());
}

}  // namespace
}  // namespace supersonic
