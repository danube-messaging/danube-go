package danube

import (
	"fmt"
	"strings"

	"github.com/danube-messaging/danube-go/proto"
)

// CompatibilityMode defines schema evolution rules.
type CompatibilityMode int

const (
	CompatibilityNone CompatibilityMode = iota
	CompatibilityBackward
	CompatibilityForward
	CompatibilityFull
)

// AsString returns the wire representation of the compatibility mode.
func (c CompatibilityMode) AsString() string {
	switch c {
	case CompatibilityNone:
		return "none"
	case CompatibilityBackward:
		return "backward"
	case CompatibilityForward:
		return "forward"
	case CompatibilityFull:
		return "full"
	default:
		return "backward"
	}
}

// ParseCompatibilityMode parses a compatibility mode string.
func ParseCompatibilityMode(value string) (CompatibilityMode, error) {
	switch strings.ToLower(value) {
	case "none":
		return CompatibilityNone, nil
	case "backward":
		return CompatibilityBackward, nil
	case "forward":
		return CompatibilityForward, nil
	case "full":
		return CompatibilityFull, nil
	default:
		return CompatibilityBackward, fmt.Errorf("unknown compatibility mode: %s", value)
	}
}

// SchemaType defines supported schema formats.
type SchemaType int

const (
	SchemaTypeBytes SchemaType = iota
	SchemaTypeString
	SchemaTypeNumber
	SchemaTypeAvro
	SchemaTypeJSONSchema
	SchemaTypeProtobuf
)

// AsString returns the wire representation of the schema type.
func (s SchemaType) AsString() string {
	switch s {
	case SchemaTypeBytes:
		return "bytes"
	case SchemaTypeString:
		return "string"
	case SchemaTypeNumber:
		return "number"
	case SchemaTypeAvro:
		return "avro"
	case SchemaTypeJSONSchema:
		return "json_schema"
	case SchemaTypeProtobuf:
		return "protobuf"
	default:
		return "bytes"
	}
}

// ParseSchemaType parses a schema type string.
func ParseSchemaType(value string) (SchemaType, error) {
	switch strings.ToLower(value) {
	case "bytes":
		return SchemaTypeBytes, nil
	case "string":
		return SchemaTypeString, nil
	case "number":
		return SchemaTypeNumber, nil
	case "avro":
		return SchemaTypeAvro, nil
	case "json_schema", "jsonschema":
		return SchemaTypeJSONSchema, nil
	case "protobuf", "proto":
		return SchemaTypeProtobuf, nil
	default:
		return SchemaTypeBytes, fmt.Errorf("unknown schema type: %s", value)
	}
}

// SchemaInfo is a user-friendly wrapper around GetSchemaResponse.
type SchemaInfo struct {
	SchemaID         uint64
	Subject          string
	Version          uint32
	SchemaType       string
	SchemaDefinition []byte
	Fingerprint      string
}

// SchemaDefinitionAsString returns the schema definition as a string.
func (s SchemaInfo) SchemaDefinitionAsString() (string, error) {
	return string(s.SchemaDefinition), nil
}

func schemaInfoFromProto(resp *proto.GetSchemaResponse) SchemaInfo {
	return SchemaInfo{
		SchemaID:         resp.GetSchemaId(),
		Subject:          resp.GetSubject(),
		Version:          resp.GetVersion(),
		SchemaType:       resp.GetSchemaType(),
		SchemaDefinition: resp.GetSchemaDefinition(),
		Fingerprint:      resp.GetFingerprint(),
	}
}
