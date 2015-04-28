package bloomsource

type SearchSourceView struct {
	Table string
	SearchId string
	SelectTypes []SearchSelectView
	Joins []SearchJoinView
	Relationships []SearchRelationshipView
}

type SearchJoinView struct {
	Table string
	SourceId string "source_id"
	DestId string "dest_id"
	As string
}

type SearchRelationshipView struct {
	Name string
	Table string
	Type string
	DestId string
	SourceId string
	SelectTypes []SearchSelect
	Using SearchJoinTableView
}

type SearchSelectView struct {
	Name string
	As string
	Type string
}

//=======


type SearchSources []SearchSource

type SearchSource struct {

	Name string
	Pivot string
	Id string
	Public bool
	SearchId string "search_id,omitempty"
	Select []string ",omitempty"
	SelectTypes []SearchSelect "select_types,omitempty"
	Joins []SearchJoin ",omitempty"
	Relationships []SearchRelationship ",omitempty"
}

type SearchSelect struct {
	Name string
	Type string
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
	SelectTypes []SearchSelect "select_types,omitempty"
	Using SearchJoinTable ",omitempty"
}