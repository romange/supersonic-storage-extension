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

#include "supersonic/contrib/storage/core/page_builder.h"

#include <string.h>
#include <memory>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "supersonic/base/memory/arena.h"
#include "supersonic/base/memory/memory.h"

namespace supersonic {
namespace {

static const uint8_t DATA[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

void TestInitialVectorsCount(int vectors_count) {
  PageBuilder page_builder(vectors_count, HeapBufferAllocator::Get());
  ASSERT_EQ(page_builder.ByteBuffersCount(), vectors_count);
}

TEST(PageBuilderTest, InitialVectorsCount) {
  TestInitialVectorsCount(0);
  TestInitialVectorsCount(5);
  TestInitialVectorsCount(21);
}


void TestResetPreservesVectorsCount(int vectors_count) {
  PageBuilder page_builder(vectors_count, HeapBufferAllocator::Get());
  page_builder.Reset();
  ASSERT_EQ(page_builder.ByteBuffersCount(), vectors_count);
}

TEST(PageBuilderTest, ResetPreservesVectorsCount) {
  TestResetPreservesVectorsCount(0);
  TestResetPreservesVectorsCount(5);
  TestResetPreservesVectorsCount(21);
}

void TestResetChangesVectorsCount(int initial_count, int new_count) {
  PageBuilder page_builder(initial_count, HeapBufferAllocator::Get());
  page_builder.Reset(new_count);
  ASSERT_EQ(page_builder.ByteBuffersCount(), new_count);
}

TEST(PageBuilderTest, ResetChangesVectorsCount) {
  TestResetChangesVectorsCount(0, 5);
  TestResetChangesVectorsCount(15, 0);
}

TEST(PageBuilderTest, InitialPageSize) {
  PageBuilder page_builder(0, HeapBufferAllocator::Get());
  ASSERT_EQ(page_builder.PageSize(), kSerializedPageHeaderSize);

  page_builder.Reset(5);
  ASSERT_EQ(page_builder.PageSize(), kSerializedPageHeaderSize +
      5 * sizeof(Page::offset_t) + 5 * kSerializedByteBufferHeaderSize);
}

TEST(PageBuilderTest, AppendToVectorIncreasesSize) {
  PageBuilder page_builder(5, HeapBufferAllocator::Get());
  char data[] = {0, 1, 2, 3, 4, 5};

  size_t page_size = page_builder.PageSize();

  page_builder.AppendToByteBuffer(0, data, 2);
  page_builder.AppendToByteBuffer(4, data, 4);

  ASSERT_EQ(page_builder.PageSize(), page_size + 6);
}

void TestValidVectorData(
    const Page &page,
    unsigned int index,
    const void* data,
    size_t expected_length) {
  FailureOr<const ByteBufferHeader*> vector_header =
      page.ByteBufferHeader(index);
  ASSERT_TRUE(vector_header.is_success());
  ASSERT_EQ(vector_header.get()->length, expected_length);

  FailureOr<const void*> vector = page.ByteBuffer(index);
  ASSERT_TRUE(vector.is_success());
  ASSERT_EQ(memcmp(vector.get(), data, vector_header.get()->length), 0);
}

TEST(PageBuilderTest, AppendThenCreatePage) {
  PageBuilder page_builder(3, HeapBufferAllocator::Get());

  page_builder.AppendToByteBuffer(0, DATA, 6);
  page_builder.AppendToByteBuffer(1, DATA + 1, 5);
  page_builder.AppendToByteBuffer(2, DATA + 2, 4);

  page_builder.AppendToByteBuffer(0, DATA + 6, 4);
  page_builder.AppendToByteBuffer(1, DATA + 6, 3);
  page_builder.AppendToByteBuffer(2, DATA + 6, 2);

  FailureOrOwned<Page> page_result = page_builder.CreatePage();
  ASSERT_TRUE(page_result.is_success());
  std::unique_ptr<Page> page(page_result.release());

  ASSERT_EQ(page->PageHeader().byte_buffers_count, 3);
  TestValidVectorData(*page.get(), 0, DATA, 10);
  TestValidVectorData(*page.get(), 1, DATA + 1, 8);
  TestValidVectorData(*page.get(), 2, DATA + 2, 6);
}

}  // namespace
}  // namespace supersonic
