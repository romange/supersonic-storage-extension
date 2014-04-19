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

#include <google/protobuf/text_format.h>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "supersonic/contrib/storage/core/storage_scan.h"

namespace supersonic {

FailureOr<TupleSchema> ReadSchema(
    ReadableStorage* storage,
    BufferAllocator* buffer_allocator);

namespace {

class MockStorage : public ReadableStorage {
 public:
  MOCK_METHOD1(CreatePageStreamReader,
               FailureOrOwned<PageStreamReader>(const std::string&));
  MOCK_METHOD1(CreateByteStreamReader,
               FailureOrOwned<ByteStreamReader>(const std::string&));
};

class MockByteStreamReader : public ByteStreamReader {
 public:
  MOCK_METHOD2(ReadBytes, FailureOr<int64_t>(void*, int64_t));
  MOCK_METHOD0(Finalize, FailureOrVoid());
};

void AssertEqAttribute(const AttributeProto& a, const Attribute& b) {
  ASSERT_EQ(a.type(), b.type());
  ASSERT_EQ(a.cardinality() == OPTIONAL, b.is_nullable());
  ASSERT_EQ(0, a.name().compare(b.name()));
}

void AssertEqSchema(const SchemaProto& a, const TupleSchema& b) {
  ASSERT_EQ(a.attribute_size(), b.attribute_count());
  for (int index = 0; index < a.attribute_size(); index++) {
    AssertEqAttribute(a.attribute(index), b.attribute(index));
  }
}

ACTION_P2(CopyBytes, src, size) {
  memcpy(arg0, src, size);
  return Success(size);
}

TEST(StorageScanTest, ReadsSchema) {
  SchemaProto schema_proto;
  AttributeProto* attribute_a = schema_proto.add_attribute();
  attribute_a->set_name("A");
  attribute_a->set_type(INT32);
  attribute_a->set_cardinality(OPTIONAL);
  AttributeProto* attribute_b = schema_proto.add_attribute();
  attribute_b->set_name("B");
  attribute_b->set_type(DOUBLE);
  attribute_b->set_cardinality(REQUIRED);

  std::string serialized_schema;
  ASSERT_TRUE(::google::protobuf::TextFormat::PrintToString(
      schema_proto, &serialized_schema));

  std::unique_ptr<MockStorage> mock_storage(new MockStorage());
  std::unique_ptr<MockByteStreamReader>
      mock_byte_stream_reader(new MockByteStreamReader);

  size_t chunk_size = serialized_schema.size() / 2;
  EXPECT_CALL(*mock_byte_stream_reader, ReadBytes(::testing::_, ::testing::_))
      .WillOnce(CopyBytes(serialized_schema.c_str(), chunk_size))
      .WillOnce(CopyBytes(serialized_schema.c_str() + chunk_size,
                          serialized_schema.size() - chunk_size))
      .WillOnce(::testing::Return(Success(0)));
  EXPECT_CALL(*mock_byte_stream_reader, Finalize())
        .WillOnce(::testing::Return(Success()));
  EXPECT_CALL(*mock_storage, CreateByteStreamReader(::testing::_))
      .WillOnce(::testing::Return(Success(mock_byte_stream_reader.release())));

  FailureOr<TupleSchema> tuple_schema_result =
      ReadSchema(mock_storage.get(), HeapBufferAllocator::Get());
  ASSERT_TRUE(tuple_schema_result.is_success());
  TupleSchema tuple_schema = tuple_schema_result.get();

  AssertEqSchema(schema_proto, tuple_schema);
}



}  // namespace

}  // namespace supersonic
