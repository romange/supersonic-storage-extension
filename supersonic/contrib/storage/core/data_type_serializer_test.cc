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

#include "supersonic/contrib/storage/core/data_type_serializer.h"

#include <numeric>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/cursor/infrastructure/table.h"

namespace supersonic {
namespace {

class DataTypeSerializerTest : public ::testing::Test {
 protected:
  template<typename T>
  void MapBuffer(const T* buffer,
                 const size_t lengths[],
                 const size_t arrays,
                 std::vector<VariantConstPointer>* mapped) {
    size_t offset = 0;
    for (size_t i = 0; i < arrays; i++) {
      mapped->emplace_back(buffer + offset);
      offset += lengths[i];
    }
    return;
  }

  template<DataType T>
  void Serialize(const typename TypeTraits<T>::cpp_type buffer[],
                 const size_t lengths[],
                 const size_t arrays,
                 std::unique_ptr<Page>* page,
                 const void** data,
                 const struct ByteBufferHeader** byte_buffer_header) {
    std::vector<VariantConstPointer> mapped_buffer;
    MapBuffer(buffer, lengths, arrays, &mapped_buffer);
    ASSERT_NE(mapped_buffer.size(), 0);

    PageBuilder page_builder(1, HeapBufferAllocator::Get());

    FailureOrOwned<Serializer> serializer_result =
        CreateSerializer(T);
    ASSERT_TRUE(serializer_result.is_success());
    std::unique_ptr<Serializer> serializer(serializer_result.release());

    serializer->Serialize(&page_builder, 0, &mapped_buffer[0], lengths, arrays);
    FailureOrOwned<Page> page_result = page_builder.CreatePage();
    ASSERT_TRUE(page_result.is_success());
    page->reset(page_result.release());

    FailureOr<const void*> data_result = (*page)->ByteBuffer(0);
    ASSERT_TRUE(data_result.is_success());
    *data = data_result.get();

    FailureOr<const struct ByteBufferHeader*> byte_buffer_header_result =
        (*page)->ByteBufferHeader(0);
    ASSERT_TRUE(byte_buffer_header_result.is_success());
    *byte_buffer_header = byte_buffer_header_result.get();
  }

  template<DataType T>
  void TestNumericTypeSerialization(
      const typename TypeTraits<T>::cpp_type buffer[],
      const size_t lengths[],
      const size_t arrays) {
    Serialize<T>(buffer, lengths, arrays, &page, &data, &byte_buffer_header);

    size_t type_size = sizeof(typename TypeTraits<T>::cpp_type);
    size_t overall_length = std::accumulate(lengths, lengths + arrays, 0)
      * type_size;
    ASSERT_EQ(overall_length, byte_buffer_header->length);
    ASSERT_EQ(0, memcmp(data, buffer, overall_length));
  }

  std::unique_ptr<Page> page;
  const void* data;
  const struct ByteBufferHeader* byte_buffer_header;
};


TEST_F(DataTypeSerializerTest, TestNumericTypes) {
  // INT32
  const TypeTraits<INT32>::cpp_type ints[] = { 1, -2, 3, -4, 5 };
  const size_t ints_lengths[] = { 3 , 2 };
  TestNumericTypeSerialization<INT32>(ints, ints_lengths, 2);

  // INT64
  const TypeTraits<INT64>::cpp_type long_ints[] = { 1L, -2L, 3L, -4L, 5L };
  const size_t long_ints_lengths[] = { 3 , 2 };
  TestNumericTypeSerialization<INT64>(long_ints, long_ints_lengths, 2);

  // UINT32
  const TypeTraits<UINT32>::cpp_type uints[] = { 1, 2, 3, 4, 5 };
  const size_t uints_lengths[] = { 3, 2 };
  TestNumericTypeSerialization<UINT32>(uints, uints_lengths, 2);

  // UINT64
  const TypeTraits<UINT32>::cpp_type long_uints[] = { 1L, 2L, 3L, 4L, 5L };
  const size_t long_uints_lengths[] = { 3, 2 };
  TestNumericTypeSerialization<UINT32>(long_uints, long_uints_lengths, 2);

  // FLOAT
  const TypeTraits<FLOAT>::cpp_type floats[] =
      { 0.1f, -0.2f, 0.3f, -0.4f, 0.5f };
  const size_t floats_lengths[] = { 3, 2 };
  TestNumericTypeSerialization<FLOAT>(floats, floats_lengths, 2);

  // DOUBLE
  const TypeTraits<DOUBLE>::cpp_type doubles[] =
      { 0.1L, -0.2L, 0.3L, -0.4L, 0.5L };
  const size_t doubles_lengths[] = { 3, 2 };
  TestNumericTypeSerialization<DOUBLE>(doubles, doubles_lengths, 2);

  // DATE
  const TypeTraits<DATE>::cpp_type dates[] = { 1L, 2L, 3L, 4L, 5L };
  const size_t dates_lengths[] = { 3, 2 };
  TestNumericTypeSerialization<DATE>(dates, dates_lengths, 2);

  // DATETIME
  const TypeTraits<DATETIME>::cpp_type datetimes[] = { 1L, 2L, 3L, 4L, 5L };
  const size_t datetimes_lengths[] = { 3, 2 };
  TestNumericTypeSerialization<DATETIME>(datetimes, datetimes_lengths, 2);
}

TEST_F(DataTypeSerializerTest, TestCppBooleans) {
  const TypeTraits<BOOL>::cpp_type bools[] = {
      true, true, true, false, true, false, true, false,    // 0b01010111
      true, false, true, true, false, true, false, true,    // 0b10101101
      false, false, true, false, true, false, false, true,  // 0b10010100
      true, true, true, false, true, true, false, true,     // 0b10110111
      true, false, true, false, true, false, true, true     // 0b11010101
    };

  const size_t bools_lengths[] = { 35, 5 };
  Serialize<BOOL>(bools, bools_lengths, 2, &page, &data, &byte_buffer_header);

  uint32_t expected[] = {
      35, 0b10110111100101001010110101010111,
      0b00000000000000000000000000000101, 5, 0b00000000000000000000000000011010
    };

  size_t expected_length = 5 * sizeof(uint32_t);
  ASSERT_EQ(expected_length, byte_buffer_header->length);
  ASSERT_EQ(0, memcmp(&expected, data, expected_length));


  const size_t bools_empty_lengths[] = { 0, 0 };
  Serialize<BOOL>(bools, bools_empty_lengths, 2, &page, &data,
      &byte_buffer_header);

  uint32_t expected_empty[] = { 0, 0 };
  size_t expected_empty_length = 2 * sizeof(uint32_t);
  ASSERT_EQ(expected_empty_length, byte_buffer_header->length);
  ASSERT_EQ(0, memcmp(&expected_empty, data, expected_empty_length));
}

TEST_F(DataTypeSerializerTest, TestVariableLength) {
  TupleSchema schema;
  schema.add_attribute(Attribute("A", BINARY, NULLABLE));
  schema.add_attribute(Attribute("B", STRING, NULLABLE));

  Table table(schema, HeapBufferAllocator::Get());
  TableRowWriter table_writer(&table);

  std::string binary_a("a\0b\0", 4);
  std::string binary_b("0x1234", 6);
  std::string binary_empty("", 0);
  std::string string_a("abcde", 5);
  std::string string_b("Hey Joe!", 8);
  std::string string_empty("", 0);
  table_writer
      .AddRow().Binary(binary_a).String(string_a)
      .AddRow().Binary(binary_b).String(string_empty)
      .AddRow().Binary(binary_empty).String(string_b)
      .CheckSuccess();

  // BINARY
  const TypeTraits<BINARY>::cpp_type* binary_data =
      table.view().column(0).typed_data<BINARY>();
  const size_t binary_lengths[] = { 2, 1 };
  Serialize<BINARY>(binary_data, binary_lengths, 2, &page, &data,
      &byte_buffer_header);

  char expected_binary[] = {
      0x02, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00,  // two arrays

      0x02, 0x00, 0x00, 0x00,  // two string pieces
      0x1A, 0x00, 0x00, 0x00,  // buffer length: 26
      'a', '\0', 'b', '\0',
      '0', 'x', '1', '2', '3', '4',
      0x06, 0x00, 0x00, 0x00, // length: 6
      0x04, 0x00, 0x00, 0x00, // length: 4


      0x01, 0x00, 0x00, 0x00,  // one string piece
      0x0C, 0x00, 0x00, 0x00,  // buffer length: 12
                               // empty content
      0x00, 0x00, 0x00, 0x00,  // length: 0
     };

  size_t expected_binary_length = 46;
  ASSERT_EQ(expected_binary_length, byte_buffer_header->length);
  ASSERT_EQ(0, memcmp(&expected_binary, data, expected_binary_length));

  // STRING
  const TypeTraits<STRING>::cpp_type* string_data =
        table.view().column(1).typed_data<STRING>();
    const size_t string_lengths[] = { 2, 1 };
    Serialize<STRING>(string_data, string_lengths, 2, &page, &data,
        &byte_buffer_header);

  char expected_string[] = {
      0x02, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00,  // two arrays

      0x02, 0x00, 0x00, 0x00,  // two string pieces
      0x15, 0x00, 0x00, 0x00,  // buffer length: 21
      'a', 'b', 'c', 'd', 'e',
      0x00, 0x00, 0x00, 0x00,  // length: 0
      0x05, 0x00, 0x00, 0x00,  // length: 5

      0x01, 0x00, 0x00, 0x00,  // one string piece
      0x14, 0x00, 0x00, 0x00,  // buffer length: 20
      'H', 'e', 'y', ' ', 'J', 'o', 'e', '!',
      0x08, 0x00, 0x00, 0x00   // length: 8
  };

  size_t expected_string_length = 49;
  ASSERT_EQ(expected_string_length, byte_buffer_header->length);
  ASSERT_EQ(0, memcmp(&expected_string, data, expected_string_length));
}


}  // namespace
}  // namespace supersonic
