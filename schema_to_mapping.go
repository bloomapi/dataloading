package bloomsource

func SchemaToMapping(schemas []SourceSchema) (*SourceMapping) {
  mappings := SourceMapping{ make([]Mapping, len(schemas)) }

	for schemaIndex, schema := range schemas {
		destFields := make([]MappingField, len(schema.Fields) + 1)

		mappings.Sources[schemaIndex] = Mapping{
			Name: schema.SourceName,
			Destinations: []Destination{
				Destination{
					Name: schema.SourceName,
					Fields: destFields,
				},
			},
		}

		keySource := make([]string, len(schema.Fields))
		for fieldIndex, field := range schema.Fields {
			keySource[fieldIndex] = field.FieldName
		}

		destFields[0] = KeyedMappingField{
			Source: keySource,
			Dest: "id",
		}

		for fieldIndex, field := range schema.Fields {
			destFields[fieldIndex + 1] = DirectMappingField{
				Source: field.FieldName,
				Dest: field.FieldName,
				Type: field.FieldType,
			}
		}
	}

	return &mappings
}