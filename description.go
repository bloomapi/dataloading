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

type Description interface {
	Available() ([]Source, error)
	FieldNames(string) ([]string, error)
	Reader(Source) (ValueReader, error)
}