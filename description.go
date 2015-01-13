package bloomsource

type Valuable interface {
	Value(string) string
}

type ValueReader interface {
	Read() (Valuable, error)
}

type Source struct {
	Name string
	Version string
}

type ByVersion []Source
func (a ByVersion) Len() int 						{ return len(a) }
func (a ByVersion) Swap(i, j int)				{ a[i], a[j] = a[j], a[i] }
func (a ByVersion) Less(i, j int) bool	{ return a[i].Version < a[j].Version }

type Description interface {
	Available() ([]Source, error)
	FieldNames(string) ([]string, error)
	Reader(Source) (ValueReader, error)
}