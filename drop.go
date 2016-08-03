package bloomsource

import (
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"github.com/gocodo/bloomdb"
	"github.com/spf13/viper"
)

func Drop() error {
	bloomdb := bloomdb.DBFromConfig(viper.GetString("sqlConnStr"), viper.GetStringSlice("searchHosts"))

	file, err := ioutil.ReadFile("dbmapping.yaml")
	if err != nil {
		return err
	}

	mapping := SourceMapping{}
	err = yaml.Unmarshal(file, &mapping)
	if err != nil {
		return err
	}

	conn, err := bloomdb.SqlConnection()
	if err != nil {
		return err
	}

	var deleteQuery string

	for _, source := range mapping.Sources {
		var sourceId string
		err := conn.QueryRow("SELECT id FROM sources WHERE name = $1", source.Name).Scan(&sourceId)
		if err != nil {
			return err
		}

		rows, err := conn.Query("SELECT name FROM source_tables WHERE source_id = $1", sourceId)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				return err
			}

			deleteQuery += "DROP TABLE IF EXISTS " + tableName + ";\n"
			deleteQuery += "DROP TABLE IF EXISTS " + tableName + "_revisions;\n"
		}

		if err := rows.Err(); err != nil {
			return err
		}

		deleteQuery += "DELETE FROM source_versions WHERE source_id = '" + sourceId + "';\n"
		deleteQuery += "DELETE FROM source_tables WHERE source_id = '" + sourceId + "';\n"
		deleteQuery += "DELETE FROM sources where id = '" + sourceId + "';\n"
	}

	_, err = conn.Exec(deleteQuery)
	if err != nil {
		return err
	}

	return nil
}