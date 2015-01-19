package bloomsource

import (
	"strconv"
	"github.com/gocodo/bloomdb"
)

var sqlTypes = map[string]string{
	"datetime": "datetime",
	"bigint": "bigint",
	"int": "int",
	"decimal": "decimal",
	"string": "character varying",
}

func MappingToCreate(mapping *SourceMapping) string {
	sources := (*mapping).Sources
	var create string

	for _, source := range sources {
		for _, destination := range source.Destinations {
			create += "CREATE TABLE " + destination.Name + "(\n"

			for fieldIndex, field := range destination.Fields {
				var sqlType string
				switch field.Source.(type) {
				case string:
					if field.Type == "" {
						sqlType = sqlTypes["string"]
					} else {
						sqlType = sqlTypes[field.Type]
					}

					if sqlType == "character varying" {
						if field.MaxLength != 0 {
							sqlType += "(" + strconv.Itoa(field.MaxLength) + ")"
						} else {
							sqlType += "(255)"
						}
					}
				case []interface{}:
					sqlType = "uuid"
				}

				create += field.Dest + " " + sqlType

				if fieldIndex + 1 != len(destination.Fields) {
					create += ",\n"
				} else {
					create += ",\n"
					create += "bloom_created_at timestamp\n"
				}
			}

			create += ");\n"
		}

		for _, destination := range source.Destinations {
			create += "CREATE TABLE " + destination.Name + "_revisions(\n"

			for fieldIndex, field := range destination.Fields {
				var sqlType string
				switch field.Source.(type) {
				case string:
					if field.Type == "" {
						sqlType = sqlTypes["string"]
					} else {
						sqlType = sqlTypes[field.Type]
					}

					if sqlType == "character varying" {
						if field.MaxLength != 0 {
							sqlType += "(" + strconv.Itoa(field.MaxLength) + ")"
						} else {
							sqlType += "(255)"
						}
					}
				case []interface{}:
					sqlType = "uuid"
				}

				create += field.Dest + " " + sqlType

				if fieldIndex + 1 != len(destination.Fields) {
					create += ",\n"
				} else {
					create += ",\n"
					create += "bloom_created_at timestamp,\n"
					create += "bloom_updated_at timestamp,\n"
					create += "bloom_action character varying(255)\n"
				}
			}

			create += ");\n"
		}

		source_id := bloomdb.MakeKey(source.Name)
		create += "INSERT INTO sources (id, name) VALUES ('" + source_id + "', '" + source.Name + "');\n";
	}

	return create;
}

func MappingToDrop(mapping *SourceMapping) string {
	sources := (*mapping).Sources
	var drop string

	for _, source := range sources {
		for _, destination := range source.Destinations {
			drop += "DROP TABLE IF EXISTS " + destination.Name + ";\n"
			drop += "DROP TABLE IF EXISTS " + destination.Name + "_revisions;\n"
		}

		drop += "DELETE FROM source_versions USING sources WHERE sources.id = source_versions.source_id AND sources.name = '" + source.Name + "';\n"
		drop += "DELETE FROM sources WHERE sources.name = '" + source.Name  + "';\n"
	}

	return drop
}

func MappingToIndex(mapping *SourceMapping) string {
	sources := (*mapping).Sources
	var index string

	for _, source := range sources {
		for _, destination := range source.Destinations {
			for _, field := range destination.Fields {
				switch field.Source.(type) {
				case []interface{}:
					index += "CREATE INDEX ON " + destination.Name + " (" + field.Dest + ");\n"
				}
			}
		}
	}

	return index
}