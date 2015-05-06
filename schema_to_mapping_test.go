package bloomsource

import (
	"testing"
	"bitbucket.org/gocodo/bloomsource/tests"
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
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Name).ToEqual("hello")
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Fields[0].Source.([]string)[0]).ToEqual("one")
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Fields[0].Dest).ToEqual("id")
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Fields[2].Source).ToEqual("one")
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Fields[2].Dest).ToEqual("one")
	spec.Expect(sourceMapping.Sources[0].Destinations[0].Fields[2].Type).ToEqual("bigint")
}

func TestFriendlyName(t *testing.T) {
	spec := tests.Spec(t)

	expects := map[string]string{
		"_hello__": "hello",
		"$Jun#kDF4": "jun_kdf4",
		"The(One)Second_#part": "the_one_second_part",
	}

	for input, expected := range expects {
		result := friendlyName(input)
		spec.Expect(result).ToEqual(expected)
	}
}