package main

import (
	"os"
	"io/ioutil"
	"github.com/bloomapi/dataloading"
	"gopkg.in/yaml.v2"
	"fmt"
)

func main() {
	pathToUpdate := os.Args[2]
	pathTypesFrom :=  os.Args[1]

	fileToUpdate, err := ioutil.ReadFile(pathToUpdate)
	if err != nil {
		fmt.Println(err)
		return
	}

	fileTypesFrom, err := ioutil.ReadFile(pathTypesFrom)
	if err != nil {
		fmt.Println(err)
		return
	}

	toUpdate := dataloading.SourceMapping{}
	err = yaml.Unmarshal(fileToUpdate, &toUpdate)
	if err != nil {
		fmt.Println(err)
		return
	}

	typesFrom := dataloading.SourceMapping{}
	err = yaml.Unmarshal(fileTypesFrom, &typesFrom)
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, toUpdateSource := range toUpdate.Sources {
		var typesFromSource *dataloading.Mapping
		for _, i := range typesFrom.Sources {
			if i.Name == toUpdateSource.Name {
				typesFromSource = &i
				break
			}
		}

		if typesFromSource == nil {
			continue
		}

		typesFromFields := map[string]dataloading.MappingField{}
		toUpdateSourceFields := []*dataloading.MappingField{}

		for _, dest := range typesFromSource.Destinations {
			for _, field := range dest.Fields {
				if source, ok := field.Source.(string); ok {
					typesFromFields[source] = field
				}
			}
		}

		for _, dest := range toUpdateSource.Destinations {
			for i := 0; i < len(dest.Fields); i++ {
				field := &dest.Fields[i]
				if _, ok := field.Source.(string); ok {
					toUpdateSourceFields = append(toUpdateSourceFields, field)
				}
			}
		}

		for _, toUpdateSourceField := range toUpdateSourceFields {
			if typesFromField, ok := typesFromFields[toUpdateSourceField.Source.(string)]; ok {
				if toUpdateSourceField.Mapping == nil {
					toUpdateSourceField.Type = typesFromField.Type
					toUpdateSourceField.MaxLength = typesFromField.MaxLength
					toUpdateSourceField.Mapping = typesFromField.Mapping
				}
			}
		}
	}

	marshaled, err := yaml.Marshal(&toUpdate)
	if err != nil {
		fmt.Println("Failed to marshal schema", err)
		os.Exit(1)
	}

	err = ioutil.WriteFile("output.yaml", marshaled, 0644)
	if err != nil {
		fmt.Println("Failed to write schema", err)
		os.Exit(1)
	}
}