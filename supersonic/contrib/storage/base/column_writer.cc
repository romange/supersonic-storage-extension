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

#include "supersonic/contrib/storage/base/column_writer.h"

#include <memory>

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/contrib/storage/base/serializer.h"
#include "supersonic/contrib/storage/core/data_type_serializer.h"

namespace supersonic {

// Writer for column holding concrete type.
class ColumnWriterImplementation : public ColumnWriter {
 public:
  ColumnWriterImplementation(std::shared_ptr<PageBuilder> page_builder,
                             int starting_from_stream,
                             DataType type,
                             bool write_is_null,
                             std::unique_ptr<Serializer> data_serializer,
                             std::unique_ptr<Serializer> is_null_serializer)
      : ColumnWriter(write_is_null ? 2 : 1),
        page_builder_(page_builder),
        starting_from_stream_(starting_from_stream),
        handled_type_(type),
        write_is_null_(write_is_null),
        data_serializer_(std::move(data_serializer)),
        is_null_serializer_(std::move(is_null_serializer)) {}

  virtual FailureOrVoid WriteColumn(const Column& column,
                                    rowcount_t row_count) {
    DCHECK(column.type_info().type() == handled_type_)
        << "Writing column of type " << column.type_info().type()
        << " into ColumnWriter for type " << handled_type_;
    DCHECK((column.attribute().nullability() == NULLABLE) == write_is_null_)
        << "Wrong column Nullability ("
        << column.attribute().nullability() << ") while writing into"
        << " ColumnWriter";

    size_t lengths[] = { row_count };
    size_t arrays = 1;

    VariantConstPointer data = column.data();
    FailureOrVoid serialize_data_result =
        data_serializer_->Serialize(page_builder_.get(),
                                    starting_from_stream_,
                                    &data,
                                    lengths,
                                    arrays);
    PROPAGATE_ON_FAILURE(serialize_data_result);

    if (write_is_null_) {
      VariantConstPointer is_null(column.is_null());
      FailureOrVoid serialize_is_null_result =
          is_null_serializer_->Serialize(page_builder_.get(),
                                         starting_from_stream_ + 1,
                                         &is_null,
                                         lengths,
                                         arrays);
      PROPAGATE_ON_FAILURE(serialize_is_null_result);
    }

    return Success();
  }

 private:
  std::shared_ptr<PageBuilder> page_builder_;
  int starting_from_stream_;
  DataType handled_type_;
  bool write_is_null_;
  std::unique_ptr<Serializer> data_serializer_;
  std::unique_ptr<Serializer> is_null_serializer_;

  DISALLOW_COPY_AND_ASSIGN(ColumnWriterImplementation);
};

FailureOrOwned<ColumnWriter> CreateColumnWriter(
    const Attribute& attribute,
    std::shared_ptr<PageBuilder> page_builder,
    int starting_from_stream) {
  bool write_is_null = attribute.nullability() == NULLABLE;

  FailureOrOwned<Serializer> data_serializer_result =
      CreateSerializer(attribute.type());
  PROPAGATE_ON_FAILURE(data_serializer_result);
  std::unique_ptr<Serializer>
      data_serializer(data_serializer_result.release());

  std::unique_ptr<Serializer> is_null_serializer;
  if (write_is_null) {
    FailureOrOwned<Serializer> is_null_serializer_result =
        CreateSerializer(BOOL);
    PROPAGATE_ON_FAILURE(is_null_serializer_result);
    is_null_serializer.reset(is_null_serializer_result.release());
  }

  return Success(new ColumnWriterImplementation(page_builder,
                                       starting_from_stream,
                                       attribute.type(),
                                       write_is_null,
                                       std::move(data_serializer),
                                       std::move(is_null_serializer)));
}

// For testing purposes.
FailureOrOwned<ColumnWriter> CreateColumnWriter(
    std::shared_ptr<PageBuilder> page_builder,
    int starting_from_stream,
    DataType type,
    bool write_is_null,
    std::unique_ptr<Serializer> data_serializer,
    std::unique_ptr<Serializer> is_null_serializer) {
  return Success(new ColumnWriterImplementation(page_builder,
                                       starting_from_stream,
                                       type,
                                       write_is_null,
                                       std::move(data_serializer),
                                       std::move(is_null_serializer)));
}

}  // namespace supersonic
