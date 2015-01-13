package bloomsource

import (
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"github.com/gocodo/bloomdb"
)

func Bootstrap () error {
	bloomdb := bloomdb.CreateDB()

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

	conn, err := bloomdb.SqlConnection()
	if err != nil {
		return err
	}

	_, err = conn.Exec(sql)
	if err != nil {
		return err
	}

	return nil
}