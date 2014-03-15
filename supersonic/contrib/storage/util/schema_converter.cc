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
//
// Classes for memory management, used by materializations
// (arenas, segments, and STL collections parametrized via arena allocators)
// so that memory usage can be controlled at the application level.
//
// Materializations can be parametrized by specifying an instance of a
// BufferAllocator. The allocator implements
// memory management policy (e.g. setting allocation limits). Allocators may
// be shared between multiple materializations; e.g. you can designate a
// single allocator per a single user request, thus setting bounds on memory

#include "supersonic/contrib/storage/util/schema_converter.h"

namespace supersonic {
namespace {

// Creates an Attribute from AttributeProto.
Attribute AttributeFromAttributeProto(
    const AttributeProto& attribute_proto) {
  Nullability nullability =
      attribute_proto.cardinality() == CardinalityType::OPTIONAL ?
          Nullability::NULLABLE : Nullability::NOT_NULLABLE;
  Attribute attribute(attribute_proto.name(),
                      attribute_proto.type(),
                      nullability);
  return attribute;
}

// Fills given mutable AttributeProto with Attribute contents.
void FillAttributeProto(AttributeProto* attribute_proto,
                        const Attribute& attribute) {
  CardinalityType cardinality =
      attribute.nullability() == Nullability::NULLABLE ?
          CardinalityType::OPTIONAL : CardinalityType::REQUIRED;

  attribute_proto->set_name(attribute.name());
  attribute_proto->set_type(attribute.type());
  attribute_proto->set_cardinality(cardinality);
}

}  // namespace


FailureOrOwned<TupleSchema> SchemaConverter::SchemaProtoToTupleSchema(
    const SchemaProto& schema_proto) {
  std::unique_ptr<TupleSchema> tuple_schema(new TupleSchema());
  for (int i = 0; i < schema_proto.attribute_size(); i++) {
    const AttributeProto& attribute_proto = schema_proto.attribute(i);
    if (!tuple_schema->add_attribute(
        AttributeFromAttributeProto(attribute_proto))) {
      THROW(new Exception(
          ERROR_GENERAL_IO_ERROR,
          StringPrintf("Unable to add attribute into TupleSchema proto.")));
    }
  }
  return Success(tuple_schema.release());
}


FailureOrOwned<SchemaProto> SchemaConverter::TupleSchemaToSchemaProto(
    const TupleSchema& tuple_schema) {
  std::unique_ptr<SchemaProto> schema_proto(new SchemaProto());
  for (size_t i = 0; i < tuple_schema.attribute_count(); i++) {
    const Attribute& attribute = tuple_schema.attribute(i);
    FillAttributeProto(schema_proto->add_attribute(), attribute);
  }
  return Success(schema_proto.release());
}

}  // namespace supersonic
