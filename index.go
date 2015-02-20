package bloomsource

import (
	"fmt"
	"log"
	"time"
	"io/ioutil"
	"encoding/json"
	"database/sql"
	"gopkg.in/yaml.v2"
	"github.com/gocodo/bloomdb"
)

func deNull(doc map[string]interface{}) {
	for k, v := range doc {
		if v == nil {
			delete(doc, k)
		} else {
			switch v.(type) {
			case map[string]interface{}:
				deNull(v.(map[string]interface{}))
			case []interface{}:
				for _, elm := range v.([]interface{}) {
					deNull(elm.(map[string]interface{}))
				}
			}
		}
	}
}

func removeNulls(doc string) (string, error) {
	var dat map[string]interface{}
	err := json.Unmarshal([]byte(doc), &dat)
	if err != nil {
		return "", err
	}
	deNull(dat)
	result, err := json.Marshal(dat)
	if err != nil {
		return "", err
	}

	return string(result), nil
}

type tableColumnInfo struct {
	Name string
	Type string
}

func tableColumns(conn *sql.DB, table string) ([]tableColumnInfo, error) {
	columns := []tableColumnInfo{}
	rows, err := conn.Query(`	SELECT column_name, data_type 
														FROM information_schema.columns
													 	WHERE table_name = '` + table + `';`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name, columnType string
		if err := rows.Scan(&name, &columnType); err != nil {
			return nil, err
		}

		columns = append(columns, tableColumnInfo{
				name,
				columnType,
			})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

func defaultColumns(columns []tableColumnInfo) []SearchSelect {
	filteredColumns := []SearchSelect{}
	for _, column := range columns {
		if column.Name != "id" && column.Name != "bloom_created_at" && column.Name != "revision" && column.Type != "uuid" {
			filteredColumns = append(filteredColumns, SearchSelect{ Name: column.Name, Type: column.Type })
		}
	}

	return filteredColumns
}

func fillSearchSourceBlanks(conn *sql.DB, mapping *SearchSource) error {
	if len(mapping.Select) == 0 {
		columns, err := tableColumns(conn, mapping.Pivot)
		if err != nil {
			return err
		}

		mapping.SelectTypes = defaultColumns(columns)
	} else {
		for _, s := range mapping.Select {
			mapping.SelectTypes = append(mapping.SelectTypes, SearchSelect{
					Name: s,
					Type: "",
				})
		}
	}

	if mapping.SearchId == "" {
		mapping.SearchId = mapping.Id
	}

	if mapping.SearchId == "id" {
		mapping.SelectTypes = append(mapping.SelectTypes, SearchSelect{
			Name: mapping.Pivot + ".id",
			Type: "uuid",
		})
	}

	if mapping.Joins == nil {
		mapping.Joins = []SearchJoin{}
	}

	if mapping.Relationships == nil {
		mapping.Relationships = []SearchRelationship{}
	}

	for i, join := range mapping.Joins {
		if join.SourceId == "" {
			mapping.Joins[i].SourceId = mapping.Id
		}

		if join.DestId == "" {
			mapping.Joins[i].DestId = "id"
		}
	}

	for i, relationship := range mapping.Relationships {
		if relationship.SourceId == "" {
			mapping.Relationships[i].SourceId = mapping.Id
		}

		if relationship.DestId == "" {
			mapping.Relationships[i].DestId = "id"
		}

		if relationship.Name == "" {
			mapping.Relationships[i].Name = relationship.Include
		}

		if len(relationship.Select) == 0 {
			// Fill Select from schema of Include table
			columns, err := tableColumns(conn, relationship.Include)
			if err != nil {
				return err
			}
			
			mapping.Relationships[i].SelectTypes = defaultColumns(columns)
		} else {
			for _, s := range relationship.Select {
				mapping.Relationships[i].SelectTypes = append(mapping.Relationships[i].SelectTypes, SearchSelect{
						Name: s,
						Type: "",
					})
			}
		}
	}

	return nil
}

func Index() error {
	startTime := time.Now().UTC()

	file, err := ioutil.ReadFile("searchmapping.yaml")
	if err != nil {
		return err
	}

	mappings := []SearchSource{}
	err = yaml.Unmarshal(file, &mappings)
	if err != nil {
		return err
	}

	bdb := bloomdb.CreateDB()
	conn, err := bdb.SqlConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	for _, mapping := range mappings {
		err = fillSearchSourceBlanks(conn, &mapping)
		if err != nil {
			return err
		}

		var lastUpdated time.Time
		err = conn.QueryRow("SELECT last_updated FROM search_types WHERE name = $1", mapping.Name).Scan(&lastUpdated)
		if err == sql.ErrNoRows {
			lastUpdated = time.Unix(0, 0)
			typeId := bloomdb.MakeKey(mapping.Name)
			_, err := conn.Exec("INSERT INTO search_types (id, name, last_updated, last_checked, public) VALUES ($1, $2, $3, $3, $4)", typeId, mapping.Name, lastUpdated, mapping.Public)
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}

		c := bdb.SearchConnection()

		indexer := c.NewBulkIndexerErrors(10, 60)
		indexer.BulkMaxBuffer = 10485760
		indexer.Start()

		indexCount := 0
		deleteCount := 0

		query := searchSourceToDeleteQuery(mapping, lastUpdated)

		rows, err := conn.Query(query)
		if err != nil {
			log.Fatal("Failed to query for rows.", err)
		}
		defer rows.Close()

		for rows.Next() {
			var id string
			err := rows.Scan(&id)
			if err != nil {
				log.Fatal(err)
			}

			deleteCount += 1
			if deleteCount % 10000 == 0 {
				fmt.Println(deleteCount, "Records Deleted in", time.Now().Sub(startTime))
			}

			indexer.Delete("source", mapping.Name, id, false)
		}

		indexer.Flush()
		fmt.Println(deleteCount, "Records Deleted in", time.Now().Sub(startTime))

		query = searchSourceToUpdateQuery(mapping, lastUpdated)
		
		insertRows, err := conn.Query(query)
		if err != nil {
			fmt.Println("Error with query:", query)
			return err
		}
		defer insertRows.Close()

		for insertRows.Next() {
			var doc, id string
			err := insertRows.Scan(&doc, &id)
			if err != nil {
				return err
			}

			doc, err = removeNulls(doc)
			if err != nil {
				return err
			}

			indexCount += 1
			if indexCount % 10000 == 0 {
				fmt.Println(indexCount, "Records Indexed in", time.Now().Sub(startTime))
			}

			indexer.Index("source", mapping.Name, id, "", nil, doc, false)
		}

		indexer.Flush()
		// There seems to be a bug in elastigo ... unsure why this sometimes fails
		// Should be fixed at some point ...
		//indexer.Stop()
		fmt.Println(indexCount, "Records Indexed in", time.Now().Sub(startTime))

		if indexCount > 0 || deleteCount > 0 {
			_, err = conn.Exec("UPDATE search_types SET last_updated = $1, last_checked = $1 WHERE name = $2", startTime, mapping.Name)
		} else {
			_, err = conn.Exec("UPDATE search_types SET last_checked = $1 WHERE name = $2", startTime, mapping.Name)
		}

		if err != nil {
			return err
		}
	}

	return nil
}