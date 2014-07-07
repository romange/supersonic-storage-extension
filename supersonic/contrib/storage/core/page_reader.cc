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

#include <vector>
#include <algorithm>

#include "supersonic/contrib/storage/base/column_reader.h"
#include "supersonic/contrib/storage/base/random_page_reader.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"


namespace supersonic {
namespace {

const rowcount_t kMaxRowCount = 8192;

typedef std::vector<std::unique_ptr<ColumnReader> > ColumnReaderVector;

class PageReaderCursor : public BasicCursor {
 public:
  PageReaderCursor(TupleSchema schema,
                   std::unique_ptr<RandomPageReader> page_reader,
                   std::unique_ptr<ColumnReaderVector> column_readers,
                   uint32_t page_family)
      : BasicCursor(schema),
        page_reader_(std::move(page_reader)),
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
    if (!eos_) {
      page_reader_->Finalize();
    }
  }

  ResultView Next(rowcount_t max_row_count) {
    rowcount_t effective_row_count = min(max_row_count, kMaxRowCount);

    if (eos_) {
      return ResultView::EOS();
    }

    if (buffered_rows_ == 0) {
      FailureOr<uint64_t> total_pages_result =
          page_reader_->TotalPages(page_family_);
      PROPAGATE_ON_FAILURE(total_pages_result);
      if (next_page_ >= total_pages_result.get()) {
        eos_ = true;
        page_reader_->Finalize();
        return ResultView::EOS();
      }
      FailureOr<const Page*> page_result =
          page_reader_->GetPage(page_family_, next_page_);
      PROPAGATE_ON_FAILURE(page_result);
      const Page& page = *page_result.get();
      PROPAGATE_ON_FAILURE(UpdateViews(page));
    }

    effective_row_count = min(effective_row_count, buffered_rows_);
    AdvanceViews(effective_row_count);

    next_page_++;
    return ResultView::Success(my_view());
  }

 private:
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

  std::unique_ptr<RandomPageReader> page_reader_;
  std::unique_ptr<ColumnReaderVector> column_readers_;
  std::vector<std::unique_ptr<View> > column_views_;
  rowcount_t buffered_rows_;
  uint32_t page_family_;
  uint64_t next_page_;
  bool eos_;
};

}  // namespace

FailureOrOwned<Cursor> PageReader(
    TupleSchema schema,
    std::unique_ptr<RandomPageReader> page_reader,
    uint32_t page_family,
    BufferAllocator* buffer_allocator) {
  // For each attribute create column reader.
  std::unique_ptr<ColumnReaderVector> column_readers(new ColumnReaderVector());

  for (int index = 0, stream = 0; index < schema.attribute_count(); index++) {
    const Attribute& attribute = schema.attribute(index);

    FailureOrOwned<ColumnReader> column_reader =
        CreateColumnReader(stream, attribute, buffer_allocator);
    PROPAGATE_ON_FAILURE(column_reader);
    stream += column_reader->uses_streams();

    column_readers->push_back(
        std::unique_ptr<ColumnReader>(column_reader.release()));
  }

  return Success(new PageReaderCursor(schema,
                                      std::move(page_reader),
                                      std::move(column_readers),
                                      page_family));
}

std::unique_ptr<Cursor>
    PageReader(TupleSchema schema,
               std::unique_ptr<RandomPageReader> page_stream,
               std::unique_ptr<ColumnReaderVector> column_readers,
               uint32_t page_family) {
  return std::unique_ptr<Cursor>(
      new PageReaderCursor(schema,
                           std::move(page_stream),
                           std::move(column_readers),
                           page_family));
}

}  // namespace supersonic
