package danube

import (
	"fmt"

	"github.com/danube-messaging/danube-go/proto"
)

// SchemaType represents the type of schema used for data serialization and validation.
// It defines the possible types of schemas that can be applied to data.
//
// Constants:
// - SchemaType_BYTES: Represents a schema where data is in raw bytes format.
// - SchemaType_STRING: Represents a schema where data is in string format.
// - SchemaType_INT64: Represents a schema where data is in 64-bit integer format.
// - SchemaType_JSON: Represents a schema where data is in JSON format.
type SchemaType int32

const (
	SchemaType_BYTES  SchemaType = 0
	SchemaType_STRING SchemaType = 1
	SchemaType_INT64  SchemaType = 2
	SchemaType_JSON   SchemaType = 3
)

// Schema represents the structure of data, including its type and associated schema data.
// It is used to define how data should be serialized, deserialized, and validated.
//
// Fields:
// - Name: The name of the schema. This is typically used for identification purposes.
// - SchemaData: The schema data itself, which contains the schema's definition. Only used with JSON TypeSchema
// - TypeSchema: The type of schema that determines the format of the data (e.g., JSON, STRING).
type Schema struct {
	Name       string
	SchemaData []byte
	TypeSchema SchemaType
}

// NewSchema creates a new Schema instance with the specified name, type, and optional JSON schema data.
// It initializes the Schema with appropriate schema data based on the type.
//
// Parameters:
// - name: The name assigned to the schema.
// - schemaType: The type of schema that determines how data is structured (e.g., JSON, STRING).
// - jsonSchema: The JSON schema data used if the schemaType is SchemaType_JSON. It is ignored for other schema types.
//
// Returns:
// - *Schema: A pointer to the newly created Schema instance.
func NewSchema(name string, schemaType SchemaType, jsonSchema string) *Schema {
	schemaData := []byte{}
	if schemaType == SchemaType_JSON {
		schemaData = []byte(jsonSchema)
	}
	return &Schema{
		Name:       name,
		SchemaData: schemaData,
		TypeSchema: schemaType,
	}
}

// Convert SchemaType to Protobuf representation
func (s SchemaType) ToProto() proto.Schema_TypeSchema {
	switch s {
	case SchemaType_BYTES:
		return proto.Schema_Bytes
	case SchemaType_STRING:
		return proto.Schema_String
	case SchemaType_INT64:
		return proto.Schema_Int64
	case SchemaType_JSON:
		return proto.Schema_JSON
	default:
		return proto.Schema_String
	}
}

// Convert Protobuf TypeSchema to SchemaType
func FromProtoTypeSchema(protoSchema proto.Schema_TypeSchema) SchemaType {
	switch protoSchema {
	case proto.Schema_Bytes:
		return SchemaType_BYTES
	case proto.Schema_String:
		return SchemaType_STRING
	case proto.Schema_Int64:
		return SchemaType_INT64
	case proto.Schema_JSON:
		return SchemaType_JSON
	default:
		return SchemaType_STRING
	}
}

// Convert Protobuf Schema to Schema
func FromProtoSchema(protoSchema *proto.Schema) (*Schema, error) {
	typeSchema := FromProtoTypeSchema(protoSchema.GetTypeSchema())
	return &Schema{
		Name:       protoSchema.GetName(),
		SchemaData: protoSchema.GetSchemaData(),
		TypeSchema: typeSchema,
	}, nil
}

// Convert Schema to Protobuf Schema
func (s *Schema) ToProto() *proto.Schema {
	return &proto.Schema{
		Name:       s.Name,
		SchemaData: s.SchemaData,
		TypeSchema: s.TypeSchema.ToProto(),
	}
}

// Convert JSON Schema to a Go string
func (s *Schema) JSONSchema() (string, error) {
	if s.TypeSchema != SchemaType_JSON {
		return "", fmt.Errorf("schema type is not JSON")
	}
	return string(s.SchemaData), nil
}

// Convert Protobuf Schema to JSON
func ProtoSchemaToJSON(protoSchema *proto.Schema) (string, error) {
	schema, err := FromProtoSchema(protoSchema)
	if err != nil {
		return "", err
	}
	return schema.JSONSchema()
}
