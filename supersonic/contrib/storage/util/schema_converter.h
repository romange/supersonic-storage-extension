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

#ifndef SUPERSONIC_CONTRIB_STORAGE_UTIL_SCHEMA_CONVERTER_H_
#define SUPERSONIC_CONTRIB_STORAGE_UTIL_SCHEMA_CONVERTER_H_

#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

// Utils for dealing with conversion between various representations of schema.
class SchemaConverter {
 public:
  // Converts a SchemaProto object into a TupleSchema object.
  static FailureOr<TupleSchema> SchemaProtoToTupleSchema(
      const SchemaProto& schema_proto);

  // Converts a TupleSchema object into a SchemaProto object.
  static FailureOrOwned<SchemaProto> TupleSchemaToSchemaProto(
      const TupleSchema& tuple_schema);
};

}  // namespace supersonic


#endif  // SUPERSONIC_CONTRIB_STORAGE_UTIL_SCHEMA_CONVERTER_H_
