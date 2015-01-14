package bloomsource

import (
	"testing"
	"github.com/gocodo/bloomsource/tests"
)

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
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Fields[0].Source.([]string)[0]).ToEqual("one")
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Fields[0].Dest).ToEqual("id")
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Fields[2].Source).ToEqual("one")
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Fields[2].Dest).ToEqual("one")
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Fields[2].Type).ToEqual("bigint")
}