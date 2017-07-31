package dataloading

import (
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"github.com/gocodo/bloomdb"
	"github.com/spf13/viper"
	"fmt"
)

func Bootstrap () error {
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

	sql := MappingToCreate(&mapping)
	indexSql := MappingToIndex(&mapping)

	conn, err := bloomdb.SqlConnection()
	if err != nil {
		return err
	}

	_, err = conn.Exec(sql)
	if err != nil {
		fmt.Println("Error executing", sql)
		return err
	}

	_, err = conn.Exec(indexSql)
	if err != nil {
		return err
	}	

	return nil
}