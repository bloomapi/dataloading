package bloomsource

type SearchSources []SearchSource

type SearchSource struct {
	Name string
	Pivot string
	Id string
	SearchId string "search_id,omitempty"
	Select []string ",omitempty"
	Joins []SearchJoin ",omitempty"
	Relationships []SearchRelationship ",omitempty"
}

type SearchJoin struct {
	Join string
	DestId string "dest_id,omitempty"
	SourceId string "source_id,omitempty"
}

type SearchJoinTable struct {
	Table string
	SourceId string "source_id"
	DestId string "dest_id"
}

type SearchRelationship struct {
	Include string
	Name string ",omitempty"
	Type string
	DestId string "dest_id,omitempty"
	SourceId string "source_id,omitempty"
	Select []string ",omitempty"
	Using SearchJoinTable ",omitempty"
}