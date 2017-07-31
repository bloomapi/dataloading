package dataloading

import (
	"io/ioutil"
	"github.com/gocodo/bloomdb"
	"gopkg.in/yaml.v2"
	"github.com/spf13/viper"
)

func IndexDrop() error {
	bdb := bloomdb.DBFromConfig(viper.GetString("sqlConnStr"), viper.GetStringSlice("searchHosts"))
	file, err := ioutil.ReadFile("searchmapping.yaml")
	if err != nil {
		return err
	}

	mappings := []SearchSource{}
	err = yaml.Unmarshal(file, &mappings)
	if err != nil {
		return err
	}

	conn, err := bdb.SqlConnection()
	if err != nil {
		return err
	}

	searchConn := bdb.SearchConnection()

	for _, source := range mappings {
		typeName := source.Name
		_, err = searchConn.DeleteIndex(source.Name)
		if err != nil {
			return err
		}

		_, err = conn.Exec("DELETE FROM search_types WHERE name = $1", typeName)
		if err != nil {
			return err
		}
	}

	return nil
}