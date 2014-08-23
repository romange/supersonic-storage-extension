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

#include <algorithm>
#include <vector>
#include <utility>

#include "supersonic/contrib/storage/base/column_reader.h"
#include "supersonic/contrib/storage/base/random_page_reader.h"
#include "supersonic/contrib/storage/util/schema_converter.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"


namespace supersonic {
namespace {

const rowcount_t kMaxRowCount = 8192;

typedef std::vector<std::unique_ptr<ColumnReader> > ColumnReaderVector;

class PageReaderCursor : public BasicCursor {
 public:
  PageReaderCursor(TupleSchema schema,
                   std::shared_ptr<RandomPageReader> page_reader,
                   std::unique_ptr<ColumnReaderVector> column_readers,
                   const PageFamily& page_family)
      : BasicCursor(schema),
        page_reader_(page_reader),
        column_readers_(std::move(column_readers)),
        buffered_rows_(0),
        page_family_(page_family),
        next_page_(0),
        eos_(false) {
    for (std::unique_ptr<ColumnReader>& column_reader : *column_readers_) {
      column_views_.emplace_back();
    }
  }

  ~PageReaderCursor() {
    // Note that page reader is shared and Finalize may be called multiple
    // times, but due to its idempotent nature everything is OK.
    // TODO(wzoltak): Not handled Failure!
    // TODO(wzoltak): should be handled by RAII.
//    page_reader_->Finalize();
  }

  ResultView Next(rowcount_t max_row_count) {
    rowcount_t effective_row_count = min(max_row_count, kMaxRowCount);

    if (eos_) {
      return ResultView::EOS();
    }

    if (buffered_rows_ == 0) {
      FailureOr<uint64_t> total_pages_result =
          page_reader_->TotalPages(page_family_.family_number());
      PROPAGATE_ON_FAILURE(total_pages_result);
      if (next_page_ >= total_pages_result.get()) {
        eos_ = true;
        return ResultView::EOS();
      }
      FailureOr<const Page*> page_result =
          page_reader_->GetPage(page_family_.family_number(), next_page_);
      PROPAGATE_ON_FAILURE(page_result);
      const Page& page = *page_result.get();
      PROPAGATE_ON_FAILURE(UpdateViews(page));
      next_page_++;
    }

    effective_row_count = min(effective_row_count, buffered_rows_);
    AdvanceViews(effective_row_count);

    return ResultView::Success(my_view());
  }

  // Seeks through data to given row. Works even after EOF (basically resets
  // the cursor). Throws if the `row` is not present in page family.
  FailureOrVoid Seek(rowcount_t row) {
    auto page_and_offset_result = FindPageAndOffset(row);
    PROPAGATE_ON_FAILURE(page_and_offset_result);

    buffered_rows_ = 0;
    next_page_ = page_and_offset_result.get().first;
    eos_ = false;

    rowcount_t rows_left = page_and_offset_result.get().second;
    do {
      ResultView skipped_data = Next(rows_left);
      PROPAGATE_ON_FAILURE(skipped_data);
      rows_left -= skipped_data.view().row_count();
    } while (rows_left > 0);
    return Success();
  }

 private:
  // For given row, returns page on which it resides along with an offset.
  FailureOr<std::pair<uint64_t, rowcount_t>> FindPageAndOffset(rowcount_t row) {
    uint64_t page_number = 0;
    rowcount_t page_row_count = page_family_.pages(page_number).row_count();
    while (true) {
      if (page_number == page_family_.pages_size()) {
        page_number = page_family_.pages_size() - 1;
        rowcount_t offset = page_family_.pages(page_number).row_count();
        return Success(std::make_pair(page_number, offset));
      }
      page_row_count = page_family_.pages(page_number).row_count();

      if (row < page_row_count) {
        break;
      }

      row -= page_row_count;
      page_number++;
    }

    return Success(std::make_pair(page_number, row));
  }

  void AdvanceViews(rowcount_t rows) {
    DCHECK(rows <= buffered_rows_);
    for (int i = 0; i < column_views_.size(); i++) {
      View* column_view = column_views_[i].get();
      my_view()->mutable_column(i)->ResetFrom(column_view->column(0));
      column_view->Advance(rows);
    }
    my_view()->set_row_count(rows);
    buffered_rows_ -= rows;
  }

  FailureOrVoid UpdateViews(const Page& page) {
    buffered_rows_ = 0;
    for (int index = 0; index < column_views_.size(); index++) {
      FailureOr<const View*> column_result =
          (*column_readers_)[index]->ReadColumn(page);
      PROPAGATE_ON_FAILURE(column_result);

      rowcount_t child_row_count = column_result.get()->row_count();

      if (buffered_rows_ != 0 && buffered_rows_ != child_row_count) {
        THROW(new Exception(
            ERROR_INVALID_STATE,
            StringPrintf("Inconsistent number of input rows from child "
                         "at index %d. Expected %lld got %lld.",
                         index,
                         buffered_rows_,
                         child_row_count)));
      }
      buffered_rows_ = child_row_count;
      column_views_[index].reset(new View(*column_result.get()));
    }
    return Success();
  }

  std::shared_ptr<RandomPageReader> page_reader_;
  std::unique_ptr<ColumnReaderVector> column_readers_;
  std::vector<std::unique_ptr<View> > column_views_;
  rowcount_t buffered_rows_;
  const PageFamily page_family_;
  uint64_t next_page_;
  bool eos_;
};

}  // namespace

FailureOrOwned<Cursor> PageReader(
    std::shared_ptr<RandomPageReader> page_reader,
    const PageFamily& page_family,
    rowcount_t starting_from_row,
    BufferAllocator* buffer_allocator) {
  auto tuple_schema_result =
      SchemaConverter::SchemaProtoToTupleSchema(page_family.schema());
  PROPAGATE_ON_FAILURE(tuple_schema_result);
  const TupleSchema& tuple_schema = tuple_schema_result.get();

  // For each attribute create column reader.
  std::unique_ptr<ColumnReaderVector> column_readers(new ColumnReaderVector());

  for (int index = 0, stream = 0;
      index < tuple_schema.attribute_count();
      index++) {
    const Attribute& attribute = tuple_schema.attribute(index);

    FailureOrOwned<ColumnReader> column_reader =
        CreateColumnReader(stream, attribute, buffer_allocator);
    PROPAGATE_ON_FAILURE(column_reader);
    stream += column_reader->uses_streams();

    column_readers->push_back(
        std::unique_ptr<ColumnReader>(column_reader.release()));
  }

  auto page_reader_cursor = new PageReaderCursor(tuple_schema,
                                                 page_reader,
                                                 std::move(column_readers),
                                                 page_family);
  PROPAGATE_ON_FAILURE(page_reader_cursor->Seek(starting_from_row));
  return Success(page_reader_cursor);
}

std::unique_ptr<Cursor>
    PageReader(TupleSchema schema,
               std::shared_ptr<RandomPageReader> page_reader,
               std::unique_ptr<ColumnReaderVector> column_readers,
               uint32_t page_family) {

  // TODO(wzoltak): fix this up
  PageFamily family;
  family.set_family_number(page_family);
  return std::unique_ptr<Cursor>(
      new PageReaderCursor(schema,
                           page_reader,
                           std::move(column_readers),
                           family));
}

}  // namespace supersonic
