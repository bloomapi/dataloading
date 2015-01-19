package bloomsource

type SourceMapping struct {
	Sources []Mapping
}

type Mapping struct {
	Name string
	Destinations []Destination
}

type Destination struct {
	Name string
	Extract string ",omitempty"
	Ignore []string ",omitempty"
	Fields []MappingField
}

type MappingField struct {
	Source interface{}
	Dest string
	Type string ",omitempty"
	MaxLength int ",omitempty"
	Mapping map[string]string ",omitempty"
}