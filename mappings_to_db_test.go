package dataloading

import (
	"testing"
	"github.com/bloomapi/dataloading/tests"
)

var sourceMapping = &SourceMapping{
	Sources: []Mapping{
		Mapping{
			Name: "source",
			Destinations: []Destination{
				Destination{
					Name: "dest",
					Fields: []MappingField{
						MappingField{
							Source: []interface{}{ "sone" },
							Dest: "id",
						},
						MappingField{
							Source: "stwo",
							Dest: "two",
							Type: "string",
							MaxLength: 50,
						},
					},
				},
			},
		},
	},
}

func TestMappingToCreate(t *testing.T) {
	spec := tests.Spec(t)
	create := MappingToCreate(sourceMapping)
	spec.Expect(create).ToEqual(`CREATE TABLE dest(
id uuid,
two character varying(50),
bloom_created_at timestamp
);
CREATE TABLE dest_revisions(
id uuid,
two character varying(50),
bloom_created_at timestamp,
bloom_updated_at timestamp,
bloom_action character varying(255)
);
INSERT INTO sources (id, name) VALUES ('c3f70f05-9179-37f5-93b8-1b8f43d291c7', 'source');
`)
}

func TestMappingToDrop(t *testing.T) {
	spec := tests.Spec(t)
	create := MappingToDrop(sourceMapping)
	spec.Expect(create).ToEqual(`DROP TABLE IF EXISTS dest;
DROP TABLE IF EXISTS dest_revisions;
DELETE FROM source_versions USING sources WHERE sources.id = source_versions.source_id AND sources.name = 'source';
DELETE FROM sources WHERE sources.name = 'source';
`)
}

func TestMappingToIndex(t *testing.T) {
	spec := tests.Spec(t)
	create := MappingToIndex(sourceMapping)
	spec.Expect(create).ToEqual(`CREATE INDEX ON dest (id);
CREATE INDEX ON dest_revisions (id);
CREATE INDEX ON dest (bloom_created_at);
CREATE INDEX ON dest_revisions (bloom_created_at);
CREATE INDEX ON dest_revisions (bloom_action);
CREATE INDEX ON dest_revisions (bloom_updated_at);
`)
}