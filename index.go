package bloomsource

import (
	"fmt"
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

func tableColumns(conn *sql.DB, table string) ([]string, error) {
	columns := []string{}
	rows, err := conn.Query(`	SELECT attname
														FROM   pg_attribute
														WHERE  attrelid = '` + table + `'::regclass
														AND    attnum > 0
														AND    NOT attisdropped
														ORDER  BY attnum;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}

		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

func removeExcludedColumns(columns []string) []string {
	filteredColumns := []string{}
	for _, column := range columns {
		if column != "id" && column != "bloom_created_at" && column != "revision" {
			filteredColumns = append(filteredColumns, column)
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

		mapping.Select = removeExcludedColumns(columns)
	}

	if mapping.SearchId == "" {
		mapping.SearchId = mapping.Id
	}

	for i, join := range mapping.Joins {
		if join.SourceId == "" {
			mapping.Joins[i].SourceId = "id"
		}

		if join.DestId == "" {
			mapping.Joins[i].DestId = "id"
		}
	}

	for i, relationship := range mapping.Relationships {
		if relationship.SourceId == "" {
			mapping.Relationships[i].SourceId = "id"
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
			
			mapping.Relationships[i].Select = removeExcludedColumns(columns)
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

	mapping := SearchSource{}
	err = yaml.Unmarshal(file, &mapping)
	if err != nil {
		return err
	}

	bdb := bloomdb.CreateDB()
	conn, err := bdb.SqlConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	err = fillSearchSourceBlanks(conn, &mapping)
	if err != nil {
		return err
	}

	var lastUpdated time.Time
	err = conn.QueryRow("SELECT last_updated FROM search_types WHERE name = $1", mapping.Name).Scan(&lastUpdated)
	if err == sql.ErrNoRows {
		lastUpdated = time.Unix(0, 0)
		conn.Exec("INSERT INTO search_types (name, last_updated) VALUES ($1, $2)", mapping.Name, lastUpdated)
	} else if err != nil {
		return err
	}

	c := bdb.SearchConnection()

	indexer := c.NewBulkIndexerErrors(10, 60)
	indexer.BulkMaxBuffer = 10485760
	indexer.Start()

	indexCount := 0
	//deleteCount := 0

	// ElastiGo currently doesn't support 'Delete' on Bulk interface ...

	/*query := searchSourceToDeleteQuery(mapping, lastUpdated)

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

		doc, err = removeNulls(doc)
		if err != nil {
			log.Fatal(err)
		}

		deleteCount += 1
		if deleteCount % 10000 == 0 {
			fmt.Println(deleteCount, "Records Deleted in", time.Now().Sub(startTime))
		}

		indexer.Delete("source", mapping.Name, id, "", nil, false)
	}

	indexer.Flush()*/

	query := searchSourceToUpdateQuery(mapping, lastUpdated)

	insertRows, err := conn.Query(query)
	if err != nil {
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

	indexer.Stop()

	_, err = conn.Exec("UPDATE search_types SET last_updated = $1 WHERE name = $2", startTime, mapping.Name)
	if err != nil {
		return err
	}

	return nil
}