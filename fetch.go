package bloomsource

import (
	"sort"
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"github.com/gocodo/bloomdb"
)

// - Get current versions of all files
// - Compare this to existing versions of files -- removed existing
// - Order sources based on version
// - Run inserter on all files to be inserted
// - Record new files that have been imported

func Fetch(desc Description) error {
	file, err := ioutil.ReadFile("dbmapping.yaml")
	if err != nil {
		return err
	}

	mapping := SourceMapping{}
	err = yaml.Unmarshal(file, &mapping)
	if err != nil {
		return err
	}

	sources, err := desc.Available()
	if err != nil {
		return err
	}

	bdb := bloomdb.CreateDB()
	conn, err := bdb.SqlConnection()
	if err != nil {
		return err
	}

	sourcesByName := make(map[string][]Source)
	var count int
	for _, source := range sources {
		if sourcesByName[source.Name] == nil {
			sourcesByName[source.Name] = []Source{}
		}

		row := conn.QueryRow("SELECT COUNT(*) FROM source_versions JOIN sources ON sources.id = source_versions.source_id WHERE sources.name = $1 AND source_versions.version = $2", source.Name, source.Version)
		err := row.Scan(&count)
		if err != nil {
			return err
		}

		if count == 0 {
			sourcesByName[source.Name] = append(sourcesByName[source.Name], source)
		}
	}

	for sourceName, sources := range sourcesByName {
		sort.Sort(ByVersion(sources))

		var currentMappingSource Mapping
		for _, mappingSource := range mapping.Sources {
			if mappingSource.Name == sourceName {
				currentMappingSource = mappingSource
				break
			}
		}

		for _, source := range sources {
			reader, err := desc.Reader(source)
			if err != nil {
				return err
			}

			fields, err := desc.FieldNames(source.Name)
			if err != nil {
				return err
			}

			var action string
			if source.Action == "" {
				action = "sync"
			} else {
				action = source.Action
			}

			err = insert(reader, currentMappingSource, fields, action)
			if err != nil {
				return err
			}

			source_id := ""
			err = conn.QueryRow("SELECT id FROM sources WHERE name = $1", source.Name).Scan(&source_id)
			if err != nil {
				return err
			}

			source_version_id := bloomdb.MakeKey(source_id, source.Version)
			_, err = conn.Exec("INSERT INTO source_versions (id, source_id, version) VALUES ($1, $2, $3)", source_version_id, source_id, source.Version)
			if err != nil {
				return err
			}
		}
	}

	downloader := NewDownloader("./data", nil)
	downloader.Clear()

	return nil
}