package bloomsource

import (
	"regexp"
)

type SourceMapping struct {
	Sources []Mapping
}

type Mapping struct {
	Name string
	Extract regexp.Regexp
	Destinations []Destination
}

type Destination struct {
	Name string
	Fields []MappingField
}

type MappingField interface {}

type KeyedMappingField struct {
	Source []string
	Dest string
}

type DirectMappingField struct {
	Source string
	Dest string
	Type string
	Mapping map[string]string
}