package dataloading

import (
	"sync"
	"github.com/gocodo/bloomdb"
	"github.com/spf13/viper"
	"io"
	"regexp"
	"strings"

	"log"
)

type fieldSet struct {
	capture string
	fields []MappingField
}

func fieldSets(d Destination, fieldNames []string) []fieldSet {
	if d.Extract != "" {
		indexes := make([]string, 0)
		extractRegexp := regexp.MustCompile(d.Extract)

		for _, fieldName := range fieldNames {
			matches := extractRegexp.FindStringSubmatch(fieldName)
			if matches != nil {
				indexes = append(indexes, matches[1])
			}
		}

		sets := make([]fieldSet, len(indexes))
		for setIndex, index := range indexes {
			set := make([]MappingField, len(d.Fields))
			for fieldIndex, field := range d.Fields {
				switch field.Source.(type) {
				case string:
					set[fieldIndex] = MappingField{
						Source: strings.Replace(field.Source.(string), "{0}", index, -1),
						Dest: field.Dest,
						Mapping: field.Mapping,
					}
				case []interface{}:
					origSources := field.Source.([]interface{})
					sources := make([]interface{}, len(origSources))
					for origIndex, origSource := range origSources {
						sources[origIndex] = strings.Replace(origSource.(string), "{0}", index, -1)
					}

					set[fieldIndex] = MappingField{
						Source: sources,
						Dest: field.Dest,
						Mapping: field.Mapping,
					}
				}
			}

			sets[setIndex] = fieldSet{
				capture: index,
				fields: set,
			}
		}

		return sets
	} else {
		return []fieldSet{
			fieldSet{
				fields: d.Fields,
			},
		}
	}
}

func writeChannel(fields []MappingField, row Valuable, output chan []string) {
	values := make([]string, len(fields))

	for index, field := range fields {
		switch field.Source.(type) {
		case string:
			value, _ := row.Value(field.Source.(string))
			/*if !ok {
				log.Println("Warning: field '" + field.Source.(string) + "' not found") 
			}*/
			if len(field.Mapping) != 0 {
				newValue, ok := field.Mapping[value]
				if ok {
					value = newValue
				}
			}

			values[index] = value
		case []interface{}:
			sources := field.Source.([]interface{})
			toKey := make([]string, len(sources))
			for sourceId, item := range sources {
				value, _ := row.Value(item.(string))
				toKey[sourceId] = value
				/*if !ok {
					log.Println("Warning: field '" + item.(string) + "' not found") 
				}*/
			}

			values[index] = bloomdb.MakeKey(toKey...)
		}
	}

	output <- values
}

func InsertWithDB(bdb *bloomdb.BloomDatabase, valueReader ValueReader, mapping Mapping, sourceNames []string, action string) error {
	var wg sync.WaitGroup

	channels := make(map[string] chan []string)

	for _, destination := range mapping.Destinations {
		channels[destination.Name] = make(chan []string, 100)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		rowCount := 0
		recordsCount := 0

		destFields := make([][]fieldSet, len(mapping.Destinations))
		for i, destination := range mapping.Destinations {
			destFields[i] = fieldSets(destination, sourceNames)
		}

		for {
			row, err := valueReader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				// TODO: what to do on error!?!?
				log.Println(err)
				return
			}

			for i, destination := range mapping.Destinations {
				for _, set := range destFields[i] {
					if len(destination.Ignore) > 0 {
						ignore := false
						for ignoreKey, ignoreValues := range destination.Ignore {
							ignoreKey = strings.Replace(ignoreKey, "{0}", set.capture, 1)
							value, _ := row.Value(ignoreKey)

							for _, ignoreValue := range ignoreValues {
								if value == ignoreValue {
									ignore = true
									break
								}
							}
						}

						if !ignore {
							writeChannel(set.fields, row, channels[destination.Name])
							recordsCount += 1
						}
					} else {
						writeChannel(set.fields, row, channels[destination.Name])
						recordsCount += 1
					}
				}
			}

			rowCount += 1

			if rowCount % 10000 == 0 {
				log.Printf("Read %d rows of %s with %d resulting records\n", rowCount, mapping.Name, recordsCount)
			}
		}

		log.Printf("Read %d rows of %s with %d resulting records\n", rowCount, mapping.Name, recordsCount)

		for _, channel := range channels {
			close(channel)
		}
	}()

	for _, destination := range mapping.Destinations {
		wg.Add(1)
		go func(destination Destination) {
			defer wg.Done()

			columns := make([]string, len(destination.Fields))
			for index, field := range destination.Fields {
				columns[index] = field.Dest
			}

			db, err := bdb.NewSqlConnection()
			if err != nil {
				// TODO: what to do on error!?!?
				log.Println(err)
				return
			}
			
			switch (action) {
			case "sync":
				err = bloomdb.Sync(db, destination.Name, columns, channels[destination.Name])
			case "upsert":
				err = bloomdb.Upsert(db, destination.Name, columns, channels[destination.Name], destination.ParentKey)
			}
			if err != nil {
				// TODO: what to do on error!?!?
				log.Println(err)
				return
			}

			err = db.Close()
			if err != nil {
				// TODO: what to do on error!?!?
				log.Println(err)
				return
			}
		}(destination)
	}

	wg.Wait()

	return nil
}

func Insert(valueReader ValueReader, mapping Mapping, sourceNames []string, action string) error {
	bdb := bloomdb.DBFromConfig(viper.GetString("sqlConnStr"), viper.GetStringSlice("searchHosts"))
	return InsertWithDB(bdb, valueReader, mapping, sourceNames, action)
}