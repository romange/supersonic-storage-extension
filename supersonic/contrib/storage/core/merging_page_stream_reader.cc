#include "supersonic/contrib/storage/core/slicing_page_stream_writer.h"

#include <google/protobuf/text_format.h>
#include <glog/logging.h>

#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/contrib/storage/base/storage.h"
#include "supersonic/contrib/storage/util/schema_converter.h"


namespace supersonic {
namespace {

class MergingPageStreamReader : public PageStreamReader {
 public:
  MergingPageStreamReader(std::unique_ptr<SuperReadableStorage> storage)
     : storage_(std::move(storage)),
       finalized_(false) {}

  virtual ~MergingPageStreamReader() {
    if (!finalized_) {
      LOG(DFATAL) << "Destroying not finalized MergingPageStream.";
      Finalize();
    }
  }

  FailureOrVoid Init() {
    return OpenNewStream();
  }

  FailureOr<const Page*> NextPage() {
    if (finalized_) {
      THROW(new Exception(
          ERROR_INVALID_STATE,
          "Writing to finalized MergingPageStream."));
    }
    const Page* page;
    // TODO(wzoltak): clean this up
    do {
      FailureOr<const Page*> page_result = page_stream_->NextPage();
      PROPAGATE_ON_FAILURE(page_result);
      if (page_result.get()->IsEmpty() && storage_->HasNext()) {
        OpenNewStream();
        // First page contains schema.
        DiscardPage();
      } else {
        page = page_result.get();
        break;
      }
    } while(true);

    return Success(page);
  }

  FailureOrVoid Finalize() {
    // TODO(wzoltak): do not set on error?
    finalized_ = true;
    return page_stream_->Finalize();
  }

 private:
  FailureOrVoid OpenNewStream() {
    if (page_stream_.get() != nullptr) {
      page_stream_->Finalize();
    }
    FailureOrOwned<PageStreamReader> page_stream_result =
        storage_->NextPageStreamReader();
    PROPAGATE_ON_FAILURE(page_stream_result);
    page_stream_.reset(page_stream_result.release());

    return Success();
  }

  FailureOrVoid DiscardPage() {
    PROPAGATE_ON_FAILURE(page_stream_->NextPage());
    return Success();
  }

  std::unique_ptr<SuperReadableStorage> storage_;
  std::unique_ptr<PageStreamReader> page_stream_;
  bool finalized_;
  DISALLOW_COPY_AND_ASSIGN(MergingPageStreamReader);
};


}  // namespace

FailureOrOwned<PageStreamReader> CreateMergingPageStreamReader(
    std::unique_ptr<SuperReadableStorage> storage) {
  std::unique_ptr<MergingPageStreamReader> merging_page_stream_reader(
      new MergingPageStreamReader(std::move(storage)));
  merging_page_stream_reader->Init();
  return Success(merging_page_stream_reader.release());
}

}  // namespace supersonic
