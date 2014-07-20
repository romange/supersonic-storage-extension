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
// Classes for file-based persistent storage. Currently, only the unix file
// system is supported. It is possible to integrate other file systems,
// e.g. distributed FS like HDFS, by creating an implementation of File and
// PathUtil interfaces.
//
// No two storage objects for single location should be active at the same time
// (either Writable or Readable).

#ifndef SUPERSONIC_CONTRIB_STORAGE_BASE_STORAGE_METADATA_H_
#define SUPERSONIC_CONTRIB_STORAGE_BASE_STORAGE_METADATA_H_

#include <memory>
#include <map>
#include <utility>
#include <vector>

#include "supersonic/base/exception/result.h"
#include "supersonic/contrib/storage/base/page.h"
#include "supersonic/contrib/storage/core/page_builder.h"
#include "supersonic/contrib/storage/util/schema_converter.h"

#include "supersonic/proto/supersonic.pb.h"


namespace supersonic {

typedef std::pair<uint32_t, const TupleSchema> Family;

// Accumulates information regarding storage matadata and is able to serialize
// it into Page object.
class MetadataWriter {
 public:
  virtual ~MetadataWriter() {}

  // Appends metadata regarding page into given family. Pages metadata within
  // single family will maintain order of method calls.
  virtual FailureOrVoid AppendPage(uint32_t family,
                                   const PageMetadata& page_metadata) = 0;

  // Dumps underlying StorageMetadata object into page.
  virtual FailureOrOwned<Page> DumpToPage() = 0;
};


// Reads a StorageMetadata object from page created using
// MetadataWriter::DumpToPage().
FailureOrOwned<StorageMetadata> ReadStorageMetadata(const Page& page);


// Creates a MetadataWriter which will store metadata of given page families.
FailureOrOwned<MetadataWriter>
    CreateMetadataWriter(const std::vector<Family>& families_schema,
                         BufferAllocator* allocator);

}  // namespace supersonic

#endif  // SUPERSONIC_CONTRIB_STORAGE_BASE_STORAGE_METADATA_H_
