#include "supersonic/contrib/storage/core/slicing_page_stream_writer.h"

#include <google/protobuf/text_format.h>
#include <glog/logging.h>

#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/contrib/storage/base/storage.h"
#include "supersonic/contrib/storage/util/schema_converter.h"


namespace supersonic {
namespace {

const size_t kMaxFileSize = 1024 * 1024; // 1MB

class SlicingPageStreamWriter : public PageStreamWriter {
 public:
  SlicingPageStreamWriter(std::unique_ptr<Page> schema_page,
                          std::unique_ptr<SuperWritableStorage> storage)
     : schema_page_(std::move(schema_page)),
       storage_(std::move(storage)),
       written_(0),
       finalized_(false) {}

  virtual ~SlicingPageStreamWriter() {
    if (!finalized_) {
      LOG(DFATAL) << "Destroying not finalized SlicingPageStream.";
      Finalize();
    }
  }

  FailureOrVoid AppendPage(const Page& page) {
    if (finalized_) {
      THROW(new Exception(
          ERROR_INVALID_STATE,
          "Writing to finalized SlicingPageStream."));
    }
    PROPAGATE_ON_FAILURE(MaybeOpenNewStream(page));
    written_ += page.PageHeader().total_size;
    return page_stream_->AppendPage(page);
  }

  FailureOrVoid Finalize() {
    // TODO(wzoltak): do not set on error?
    finalized_ = true;
    return page_stream_->Finalize();
  }

 private:
  FailureOrVoid MaybeOpenNewStream(const Page& page) {
    const bool no_page_stream = page_stream_.get() == nullptr;
    const bool empty_page = written_ == 0;
    const bool too_much_data =
        written_ + page.PageHeader().total_size > kMaxFileSize;
    if (no_page_stream || (!empty_page && too_much_data)) {
      if (page_stream_.get() != nullptr) {
        PROPAGATE_ON_FAILURE(page_stream_->Finalize());
      }

      FailureOrOwned<PageStreamWriter> page_stream_result =
          storage_->NextPageStreamWriter();
      PROPAGATE_ON_FAILURE(page_stream_result);
      page_stream_.reset(page_stream_result.release());

      written_ = 0;

      return AppendPage(*schema_page_);
    }

    return Success();
  }

  std::unique_ptr<Page> schema_page_;
  std::unique_ptr<SuperWritableStorage> storage_;
  std::unique_ptr<PageStreamWriter> page_stream_;
  size_t written_;
  bool finalized_;
  DISALLOW_COPY_AND_ASSIGN(SlicingPageStreamWriter);
};

FailureOrOwned<Page> CreateSchemaPage(
    const TupleSchema& schema,
    BufferAllocator* allocator) {
  PageBuilder page_builder(1, allocator);

  std::string serialized_schema;
  FailureOrOwned<SchemaProto> schema_proto_result =
      SchemaConverter::TupleSchemaToSchemaProto(schema);
  PROPAGATE_ON_FAILURE(schema_proto_result);
  ::google::protobuf::TextFormat::PrintToString(*schema_proto_result,
                                                &serialized_schema);

  page_builder.AppendToByteBuffer(0,
                                  serialized_schema.c_str(),
                                  serialized_schema.length());

  return page_builder.CreatePage();
}

}  // namespace

FailureOrOwned<PageStreamWriter> CreateSlicingPageStreamWriter(
    TupleSchema schema,
    std::unique_ptr<SuperWritableStorage> storage,
    BufferAllocator* allocator) {
  FailureOrOwned<Page> schema_page_result =
      CreateSchemaPage(schema, allocator);
  PROPAGATE_ON_FAILURE(schema_page_result);
  std::unique_ptr<Page> schema_page(schema_page_result.release());

  return Success(new SlicingPageStreamWriter(std::move(schema_page),
                                             std::move(storage)));
}

}  // namespace supersonic
