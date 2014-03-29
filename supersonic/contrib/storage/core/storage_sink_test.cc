// Copyright 2014 Wojciech Żółtak. All Rights Reserved.
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

#include "supersonic/contrib/storage/core/storage_sink.h"

#include <google/protobuf/text_format.h>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/contrib/storage/base/storage.h"
#include "supersonic/contrib/storage/core/page_sink.h"
#include "supersonic/contrib/storage/util/schema_converter.h"
#include "supersonic/cursor/infrastructure/table.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "supersonic/utils/exception/failureor.h"


namespace supersonic {

FailureOrVoid DumpSchema(
    const TupleSchema& schema,
    Storage* storage,
    BufferAllocator* buffer_allocator);

FailureOrOwned<Sink> CreateStorageSink(
    std::unique_ptr<std::vector<std::unique_ptr<Sink> > > page_sinks,
    std::unique_ptr<Storage> storage);

namespace {

class MockPageSink : public Sink {
 public:
  MOCK_METHOD1(Write, FailureOr<rowcount_t>(const View& data));
  MOCK_METHOD0(Finalize, FailureOrVoid());

  MockPageSink* ExpectFinalize() {
    EXPECT_CALL(*this, Finalize()).WillOnce(::testing::Return(Success()));
    return this;
  }

  MockPageSink* ExpectWrite(const View& view) {
    EXPECT_CALL(*this, Write(::testing::Ref(view)))
        .WillOnce(::testing::Return(Success(view.row_count())));
    return this;
  }
};

class MockStorage : public Storage {
 public:
  MOCK_METHOD1(CreatePageStream,
               FailureOrOwned<PageStreamWriter>(const std::string& name));
  MOCK_METHOD1(CreateByteStream,
               FailureOrOwned<ByteStreamWriter>(const std::string& name));
};

class MockByteStreamWriter : public ByteStreamWriter {
 public:
  MOCK_METHOD2(AppendBytes, FailureOrVoid(const void* buffer, size_t length));
  MOCK_METHOD0(Finalize, FailureOrVoid());

  MockByteStreamWriter* ExpectingFinalize() {
    EXPECT_CALL(*this, Finalize()).WillOnce(::testing::Return(Success()));
    return this;
  }
};

class StorageSinkTest : public ::testing::Test {
 protected:
  TupleSchema CreateTupleSchema() {
    TupleSchema schema;
    schema.add_attribute(Attribute("A", INT32, NULLABLE));
    schema.add_attribute(Attribute("B", BOOL, NOT_NULLABLE));
    return schema;
  }

  std::string Serialize(const SchemaProto& schema) {
    std::string serialized_schema;
    ::google::protobuf::TextFormat::PrintToString(schema,
                                                  &serialized_schema);
    return serialized_schema;
  }
};

MATCHER_P2(EqualsBuffer, buffer, length, "") {
  return memcmp(arg, buffer, length) == 0;
}

TEST_F(StorageSinkTest, WritesMetadata) {
  std::unique_ptr<MockStorage> storage(new MockStorage());
  std::unique_ptr<MockByteStreamWriter>
      byte_stream((new MockByteStreamWriter())->ExpectingFinalize());

  TupleSchema tuple_schema = CreateTupleSchema();
  FailureOrOwned<SchemaProto> schema_proto_result =
      SchemaConverter::TupleSchemaToSchemaProto(tuple_schema);
  ASSERT_TRUE(schema_proto_result.is_success());
  std::unique_ptr<SchemaProto> schema_proto(schema_proto_result.release());
  std::string serialized_schema = Serialize(*schema_proto);

  EXPECT_CALL(*byte_stream, AppendBytes(
      EqualsBuffer(serialized_schema.c_str(), serialized_schema.length()),
      serialized_schema.length()))
          .WillOnce(::testing::Return(Success()));
  EXPECT_CALL(*storage, CreateByteStream(::testing::_))
            .WillOnce(::testing::Return(Success(byte_stream.release())));

  ASSERT_TRUE(DumpSchema(tuple_schema, storage.get(),
                         HeapBufferAllocator::Get()).is_success());
}

TEST_F(StorageSinkTest, WritingToFinalizedThrows) {
  std::unique_ptr<std::vector<std::unique_ptr<Sink> > > page_sinks(
      new std::vector<std::unique_ptr<Sink> >());
  std::unique_ptr<Storage> storage(new MockStorage());
  TupleSchema schema;
  Table table(schema, HeapBufferAllocator::Get());

  FailureOrOwned<Sink> storage_sink_result =
      CreateStorageSink(std::move(page_sinks), std::move(storage));
  ASSERT_TRUE(storage_sink_result.is_success());
  std::unique_ptr<Sink> storage_sink(storage_sink_result.release());

  ASSERT_TRUE(storage_sink->Finalize().is_success());
  ASSERT_TRUE(storage_sink->Write(table.view()).is_failure());
}

TEST_F(StorageSinkTest, FinalizesAffectsPageSinks) {
  std::unique_ptr<std::vector<std::unique_ptr<Sink> > > page_sinks(
      new std::vector<std::unique_ptr<Sink> >());
  page_sinks->emplace_back((new MockPageSink())->ExpectFinalize());
  page_sinks->emplace_back((new MockPageSink())->ExpectFinalize());

  std::unique_ptr<Storage> storage(new MockStorage());
  TupleSchema schema = CreateTupleSchema();

  FailureOrOwned<Sink> storage_sink_result =
      CreateStorageSink(std::move(page_sinks), std::move(storage));
  ASSERT_TRUE(storage_sink_result.is_success());
  std::unique_ptr<Sink> storage_sink(storage_sink_result.release());

  ASSERT_TRUE(storage_sink->Finalize().is_success());
}

TEST_F(StorageSinkTest, DataIsPassedToPageSinks) {
  TupleSchema schema = CreateTupleSchema();
  Table table(schema, HeapBufferAllocator::Get());
  const View& view = table.view();
  std::unique_ptr<std::vector<std::unique_ptr<Sink> > > page_sinks(
      new std::vector<std::unique_ptr<Sink> >());
  page_sinks->emplace_back(
      (new MockPageSink())->ExpectFinalize()->ExpectWrite(view));
  page_sinks->emplace_back(
      (new MockPageSink())->ExpectFinalize()->ExpectWrite(view));
  std::unique_ptr<Storage> storage(new MockStorage());

  FailureOrOwned<Sink> storage_sink_result =
      CreateStorageSink(std::move(page_sinks), std::move(storage));
  ASSERT_TRUE(storage_sink_result.is_success());
  std::unique_ptr<Sink> storage_sink(storage_sink_result.release());

  ASSERT_TRUE(storage_sink->Write(table.view()).is_success());
  ASSERT_TRUE(storage_sink->Finalize().is_success());
}

}  // namespace
}  // namespace supersonic
