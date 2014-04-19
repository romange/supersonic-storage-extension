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

#include <memory>
#include <utility>

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/contrib/storage/base/deserializer.h"
#include "supersonic/contrib/storage/core/data_type_deserializer.h"
#include "supersonic/utils/exception/failureor.h"

namespace supersonic {
namespace {

class ColumnReaderImplementation : public ColumnReader {
 public:
  ColumnReaderImplementation(int first_buffer,
                             std::unique_ptr<View> view,
                             std::unique_ptr<Deserializer> data_deserializer,
                             std::unique_ptr<Deserializer> is_null_deserializer)
     : ColumnReader(view->schema().attribute(0).is_nullable() ? 2 : 1),
       data_buffer_(first_buffer),
       is_null_buffer_(data_buffer_ + 1),
       view_(std::move(view)),
       data_deserializer_(std::move(data_deserializer)),
       is_null_deserializer_(std::move(is_null_deserializer)) {}

  virtual FailureOr<const View*> ReadColumn(const Page& page) {
    Column* column = view_->mutable_column(0);

    FailureOr<pair<VariantConstPointer, rowcount_t>> deserialized_data =
        DeserializeBuffer(page,
                          data_buffer_,
                          data_deserializer_.get());
    PROPAGATE_ON_FAILURE(deserialized_data);
    VariantConstPointer data_ptr = deserialized_data.get().first;
    rowcount_t data_rowcount = deserialized_data.get().second;

    // Does not work for bitmasks.
    bool_const_ptr is_null_ptr = nullptr;
    if (view_->schema().attribute(0).is_nullable()) {
      FailureOr<pair<VariantConstPointer, rowcount_t> > deserialized_is_null =
          DeserializeBuffer(page,
                            is_null_buffer_,
                            is_null_deserializer_.get());
      PROPAGATE_ON_FAILURE(deserialized_is_null);
      is_null_ptr = deserialized_is_null.get().first.as<BOOL>();
      rowcount_t is_null_rowcount = deserialized_is_null.get().second;

      if (data_rowcount != is_null_rowcount) {
        THROW(new Exception(ERROR_INVALID_STATE,
                            "Inconsistent number of data and nulls in input."));
      }
    }

    column->Reset(data_ptr, is_null_ptr);
    view_->set_row_count(data_rowcount);
    return Success(view_.get());
  }

 private:
  FailureOr<pair<VariantConstPointer, rowcount_t> >
      DeserializeBuffer(const Page& page,
                        int buffer_index,
                        Deserializer* using_deserializer) {
    FailureOr<const void*> buffer_result =
        page.ByteBuffer(buffer_index);
    PROPAGATE_ON_FAILURE(buffer_result);
    FailureOr<const ByteBufferHeader*> buffer_head_result =
        page.ByteBufferHeader(buffer_index);
    PROPAGATE_ON_FAILURE(buffer_head_result);

    return using_deserializer->Deserialize(buffer_result.get(),
                                           *buffer_head_result.get());
  }

  int data_buffer_;
  int is_null_buffer_;
  std::unique_ptr<View> view_;
  std::unique_ptr<Deserializer> data_deserializer_;
  std::unique_ptr<Deserializer> is_null_deserializer_;

  DISALLOW_COPY_AND_ASSIGN(ColumnReaderImplementation);
};

}  // namespace

FailureOrOwned<ColumnReader> CreateColumnReader(int first_buffer,
                                                const Attribute& attribute,
                                                BufferAllocator* allocator) {
  TupleSchema schema;
  schema.add_attribute(attribute);
  std::unique_ptr<View> view(new View(schema));

  FailureOrOwned<Deserializer> data_deserializer_result =
      CreateDeserializer(attribute.type(), allocator);
  PROPAGATE_ON_FAILURE(data_deserializer_result);
  std::unique_ptr<Deserializer> data_deserializer(
      data_deserializer_result.release());

  // Does not work with bitmask nulls.
  std::unique_ptr<Deserializer> is_null_deserializer;
  if (attribute.is_nullable()) {
    FailureOrOwned<Deserializer> is_null_deserializer_result =
        CreateDeserializer(BOOL, allocator);
    PROPAGATE_ON_FAILURE(is_null_deserializer_result);
    is_null_deserializer.reset(is_null_deserializer_result.release());
  }

  return Success(
      new ColumnReaderImplementation(first_buffer,
                                     std::move(view),
                                     std::move(data_deserializer),
                                     std::move(is_null_deserializer)));
}


// For testing purposes.
std::unique_ptr<ColumnReader> CreateColumnReader(
    int starting_from_stream,
    std::unique_ptr<View> view,
    std::unique_ptr<Deserializer> data_deserializer,
    std::unique_ptr<Deserializer> is_null_deserializer) {
  return std::unique_ptr<ColumnReader>(
      new ColumnReaderImplementation(starting_from_stream,
                                     std::move(view),
                                     std::move(data_deserializer),
                                     std::move(is_null_deserializer)));
}

}  // namespace supersonic
