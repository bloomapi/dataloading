package bloomsource

import (
	"testing"
	"github.com/gocodo/bloomsource/tests"
)

//func SchemaToMapping(schemas []SourceSchema) (*SourceMapping) {
func TestSchemaToMapping(t *testing.T) {
	spec := tests.Spec(t)

	schemas := []SourceSchema{
		SourceSchema{
			SourceName: "Hello",
			Fields: []FieldInfo{
				FieldInfo{
					FieldName: "one",
					FieldType: "bigint",
				},
			},
		},
	}

	sourceMapping := *SchemaToMapping(schemas)

	spec.Expect(sourceMapping.Sources[0].Name).ToEqual("Hello")
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Name).ToEqual("Hello")
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Fields[0].(KeyedMappingField).Source[0]).ToEqual("one")
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Fields[0].(KeyedMappingField).Dest).ToEqual("id")
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Fields[1].(DirectMappingField).Source).ToEqual("one")
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Fields[1].(DirectMappingField).Dest).ToEqual("one")
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Fields[1].(DirectMappingField).Type).ToEqual("bigint")
}