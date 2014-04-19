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

#include <string>

#include "gtest/gtest.h"

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/contrib/storage/util/schema_converter.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {
namespace {

void AssertEqualSchemas(const TupleSchema& tuple_schema,
                        const SchemaProto& schema_proto) {
  ASSERT_EQ(tuple_schema.attribute_count(), schema_proto.attribute_size());
  for (int i = 0; i < tuple_schema.attribute_count(); i++) {
    ASSERT_STREQ(tuple_schema.attribute(i).name().c_str(),
                 schema_proto.attribute(i).name().c_str());
    ASSERT_EQ(tuple_schema.attribute(i).type(),
              schema_proto.attribute(i).type());
    ASSERT_EQ(tuple_schema.attribute(i).nullability() == NULLABLE,
              schema_proto.attribute(i).cardinality() == OPTIONAL);
  }
}

void FillAttribute(AttributeProto* attribute,
                  const std::string& name,
                  DataType type,
                  CardinalityType cardinality) {
  attribute->set_name(name);
  attribute->set_type(type);
  attribute->set_cardinality(cardinality);
}

TEST(SchemaConverter, TupleSchemaToSchemaProto) {
  TupleSchema tuple_schema;
  tuple_schema.add_attribute(Attribute("A", DataType::INT64, NULLABLE));
  tuple_schema.add_attribute(Attribute("B", DataType::BOOL, NOT_NULLABLE));

  FailureOrOwned<SchemaProto> proto_schema =
      SchemaConverter::TupleSchemaToSchemaProto(tuple_schema);
  ASSERT_TRUE(proto_schema.is_success());
  AssertEqualSchemas(tuple_schema, *proto_schema);
}

TEST(SchemaConverter, SchemaProtoToTupleSchema) {
  SchemaProto schema_proto;
  FillAttribute(schema_proto.add_attribute(), "A", DataType::INT64, OPTIONAL);
  FillAttribute(schema_proto.add_attribute(), "B", DataType::BOOL, REQUIRED);

  FailureOr<TupleSchema> tuple_schema_result =
      SchemaConverter::SchemaProtoToTupleSchema(schema_proto);
  ASSERT_TRUE(tuple_schema_result.is_success());
  AssertEqualSchemas(tuple_schema_result.get(), schema_proto);
}

}  // namespace
}  // namespace supersonic
