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
	ParentKey string "parent_key,omitempty"
	Extract string ",omitempty"
	Ignore map[string][]string ",omitempty"
	Fields []MappingField
}

type MappingField struct {
	Source interface{}
	Dest string
	Type string ",omitempty"
	MaxLength int "max_length,omitempty"
	Mapping map[string]string ",omitempty"
}