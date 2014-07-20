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
//
// Storage is represented by series of page readers / writers. Those can be used
// to split storage into logical chunks. E.g. in case of file storage - each
// of them may represent a single file. Each of those "chunks" should be usable
// as a storage on its own.

#include "supersonic/contrib/storage/base/storage_metadata.h"

#include <vector>

namespace supersonic {
namespace {

class MetadataWriterImplementation : public MetadataWriter {
 public:
  MetadataWriterImplementation(
      std::unique_ptr<map<uint32_t, int>> families_map,
      std::unique_ptr<StorageMetadata> metadata,
      BufferAllocator* allocator)
          : families_map_(std::move(families_map)),
            metadata_(std::move(metadata)),
            page_builder_(1, allocator) {}

  FailureOrVoid AppendPage(uint32_t family,
                                   const PageMetadata& page_metadata) {
    auto it = families_map_->find(family);
    if (it == families_map_->end()) {
      THROW(new Exception(
          ERROR_INVALID_ARGUMENT_VALUE,
          StringPrintf("Unknown page family %d", family)));
    }
    PageFamily* family_metadata = metadata_->mutable_page_families(it->second);
    family_metadata->add_pages()->MergeFrom(page_metadata);
    return Success();
  }

  FailureOrOwned<Page> DumpToPage() {
    page_builder_.Reset();

    int size = metadata_->ByteSize();
    auto buffer_result =
        page_builder_.NextFromByteBuffer(0, size);
    PROPAGATE_ON_FAILURE(buffer_result);
    if (!metadata_->SerializeToArray(buffer_result.get(), size)) {
      THROW(new Exception(ERROR_BAD_PROTO,
                          "Can not serialize StorageMetadata proto."));
    }

    return page_builder_.CreatePage();
  }

 private:
  std::unique_ptr<std::map<uint32_t, int>> families_map_;
  std::unique_ptr<StorageMetadata> metadata_;
  PageBuilder page_builder_;
  DISALLOW_COPY_AND_ASSIGN(MetadataWriterImplementation);
};

}  // namespace


FailureOrOwned<StorageMetadata> ReadStorageMetadata(const Page& page) {
  std::unique_ptr<StorageMetadata> metadata(new StorageMetadata());

  auto byte_buffer_header_result = page.ByteBufferHeader(0);
  PROPAGATE_ON_FAILURE(byte_buffer_header_result);
  auto byte_buffer_result = page.ByteBuffer(0);
  PROPAGATE_ON_FAILURE(byte_buffer_result);

  metadata->ParseFromArray(byte_buffer_result.get(),
                           byte_buffer_header_result.get()->length);

  return Success(metadata.release());
}


FailureOrOwned<MetadataWriter>
    CreateMetadataWriter(const std::vector<Family>& families_schema,
                         BufferAllocator* allocator) {
  std::unique_ptr<map<uint32_t, int>> families_map(new map<uint32_t, int>());
  std::unique_ptr<StorageMetadata> metadata(new StorageMetadata());
  for (auto& family : families_schema) {
    auto schema_proto_result =
        SchemaConverter::TupleSchemaToSchemaProto(family.second);
    PROPAGATE_ON_FAILURE(schema_proto_result);

    (*families_map)[family.first] = metadata->page_families_size();
    PageFamily* family_metadata = metadata->add_page_families();
    family_metadata->set_family_number(family.first);
    family_metadata->set_allocated_schema(schema_proto_result.release());
  }

  return Success(new MetadataWriterImplementation(std::move(families_map),
                                                  std::move(metadata),
                                                  allocator));
}

}  // namespace supersonic
