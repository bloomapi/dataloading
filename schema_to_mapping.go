package dataloading

import (
	"strings"
	"regexp"
)

var nonFriendlyCharacters = regexp.MustCompile(`[^a-zA-Z0-9_]+`)
var tooManyUnderscores = regexp.MustCompile(`_+`)

func friendlyName(name string) string {
	friendly := strings.ToLower(name)
	friendly = nonFriendlyCharacters.ReplaceAllString(friendly, "_")
	friendly = tooManyUnderscores.ReplaceAllString(friendly, "_")
	friendly = strings.Trim(friendly, "_")
	return friendly
}

func SchemaToMapping(schemas []SourceSchema) (*SourceMapping) {
  mappings := SourceMapping{ make([]Mapping, len(schemas)) }

	for schemaIndex, schema := range schemas {
		destFields := make([]MappingField, len(schema.Fields) + 2)

		mappings.Sources[schemaIndex] = Mapping{
			Name: schema.SourceName,
			Destinations: []Destination{
				Destination{
					Name: friendlyName(schema.SourceName),
					Fields: destFields,
				},
			},
		}

		keySource := make([]string, len(schema.Fields))
		for fieldIndex, field := range schema.Fields {
			keySource[fieldIndex] = field.FieldName
		}

		destFields[0] = MappingField{
			Source: keySource,
			Dest: "id",
		}

		destFields[1] = MappingField{
			Source: keySource,
			Dest: "revision",
		}

		for fieldIndex, field := range schema.Fields {
			destFields[fieldIndex + 2] = MappingField{
				Source: field.FieldName,
				Dest: friendlyName(field.FieldName),
				Type: field.FieldType,
			}

			if field.FieldType == "string" {
				destFields[fieldIndex + 2].MaxLength = field.MaxLength * 2
			}
		}
	}

	return &mappings
}