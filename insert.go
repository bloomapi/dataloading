package bloomsource

import (
	"sync"
	"github.com/gocodo/bloomdb"
	"io"
	"regexp"
	"strings"

	"fmt"
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
				case []string:
					origSources := field.Source.([]string)
					sources := make([]string, len(origSources))
					for origIndex, origSource := range origSources {
						sources[origIndex] = strings.Replace(origSource, "{0}", index, -1)
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
			value := row.Value(field.Source.(string))
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
				toKey[sourceId] = row.Value(item.(string))
			}

			values[index] = bloomdb.MakeKey(toKey...)
		}
	}

	output <- values
}

func insert(valueReader ValueReader, mapping Mapping, sourceNames []string) error {
	var wg sync.WaitGroup

	channels := make(map[string] chan []string)

	for _, destination := range mapping.Destinations {
		channels[destination.Name] = make(chan []string, 100)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

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
				fmt.Println(err)
				return
			}

			for i, destination := range mapping.Destinations {
				for _, set := range destFields[i] {
					if len(destination.Ignore) > 0 {
						// TODO: Implement
					} else {
						writeChannel(set.fields, row, channels[destination.Name])
					}
				}
			}
		}

		for _, channel := range channels {
			close(channel)
		}
	}()

	bdb := bloomdb.CreateDB()
	for _, destination := range mapping.Destinations {
		wg.Add(1)
		go func(destination Destination) {
			defer wg.Done()

			columns := make([]string, len(destination.Fields))
			for index, field := range destination.Fields {
				columns[index] = field.Dest
			}

			db, err := bdb.SqlConnection()
			if err != nil {
				// TODO: what to do on error!?!?
				fmt.Println(err)
				return
			}
			
			err = bloomdb.Sync(db, destination.Name, columns, channels[destination.Name])
			if err != nil {
				// TODO: what to do on error!?!?
				fmt.Println(err)
				return
			}
		}(destination)
	}

	wg.Wait()

	return nil
}