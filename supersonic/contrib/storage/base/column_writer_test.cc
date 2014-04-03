#include "supersonic/contrib/storage/base/column_writer.h"

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/contrib/storage/base/serializer.h"
#include "supersonic/cursor/infrastructure/table.h"

namespace supersonic {

FailureOrOwned<ColumnWriter> CreateColumnWriter(
    std::shared_ptr<PageBuilder> page_builder,
    int starting_from_stream,
    DataType type,
    bool write_is_null,
    std::unique_ptr<Serializer> data_serializer,
    std::unique_ptr<Serializer> is_null_serializer);

namespace {

MATCHER_P(FirstVarPtrEq, ptr, "") { return arg[0].raw() == ptr; }
MATCHER_P(FirstElEq, el, "") { return arg[0] == el; }

template <DataType T>
class MockSerializer : public Serializer {
 public:
  typedef typename TypeTraits<T>::cpp_type CppType;
  MOCK_METHOD5_T(Serialize, FailureOrVoid(PageBuilder*,
                                          int,
                                          VariantConstPointer[],
                                          const size_t[],
                                          const size_t));

  MockSerializer* ExpectingSerialize(PageBuilder* page_builder,
                                     int stream_index,
                                  const CppType* data,
                                  const size_t length) {
    EXPECT_CALL(*this, Serialize(page_builder,
                                 stream_index,
                                 FirstVarPtrEq(data),
                                 FirstElEq(length),
                                 1)).WillOnce(::testing::Return(Success()));
    return this;
  }
};

TEST(ColumnWriterTest, PassesDataToSerializers) {
  TupleSchema schema;
  schema.add_attribute(Attribute("A", INT32, NULLABLE));

  std::shared_ptr<PageBuilder>
      page_builder(new PageBuilder(2, HeapBufferAllocator::Get()));

  Table table(schema, HeapBufferAllocator::Get());
  const TypeTraits<INT32>::cpp_type* data =
      table.view().column(0).typed_data<INT32>();
  const TypeTraits<BOOL>::cpp_type* is_null = table.view().column(0).is_null();

  std::unique_ptr<MockSerializer<INT32> > data_serializer(
      (new MockSerializer<INT32>())
          ->ExpectingSerialize(page_builder.get(), 0, data, 0));
  std::unique_ptr<MockSerializer<BOOL> > is_null_serializer(
      (new MockSerializer<BOOL>())
          ->ExpectingSerialize(page_builder.get(), 1, is_null, 0));

  FailureOrOwned<ColumnWriter> column_writer =
      CreateColumnWriter(page_builder,
                         0,
                         INT32,
                         true,
                         std::move(data_serializer),
                         std::move(is_null_serializer));
  ASSERT_TRUE(column_writer.is_success());

  column_writer->WriteColumn(table.view().column(0), 0).is_success();
}

TEST(ColumnWriterTest, IncompatibleColumnFails) {
  TupleSchema schema;
  schema.add_attribute(Attribute("A", INT32, NULLABLE));
  schema.add_attribute(Attribute("B", FLOAT, NULLABLE));
  schema.add_attribute(Attribute("C", INT32, NOT_NULLABLE));
  Table table(schema, HeapBufferAllocator::Get());

  const Column& incompatible_type_column = table.view().column(1);
  const Column& incompatible_nullability_column = table.view().column(2);

  std::shared_ptr<PageBuilder>
      page_builder(new PageBuilder(2, HeapBufferAllocator::Get()));

  FailureOrOwned<ColumnWriter> column_writer_result =
      CreateColumnWriter(schema.attribute(0), page_builder, 0);
  ASSERT_TRUE(column_writer_result.is_success());
  std::unique_ptr<ColumnWriter> column_writer(column_writer_result.release());

  EXPECT_DEBUG_DEATH(
      column_writer->WriteColumn(incompatible_type_column, 0),
      StringPrintf("Writing column of type %d into ColumnWriter for type %d",
                   FLOAT, INT32));
  EXPECT_DEBUG_DEATH(
        column_writer->WriteColumn(incompatible_nullability_column, 0),
        "Wrong column Nullability \\(0) while writing into ColumnWriter");
}

}  // namespace
}  // namespace supersonic
