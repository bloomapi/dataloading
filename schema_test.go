package bloomsource

import (
	"testing"
	"io"
	"github.com/gocodo/bloomsource/tests"
)

type TestReader struct {
	looped int
}

func (r *TestReader) Read() (Valuable, error) {
	r.looped += 1
	if r.looped > 10 {
		return nil, io.EOF
	}

	v := &TestValuable{}
	return v, nil
}

type TestValuable struct {}

func (v *TestValuable) Value(key string) string {
	switch (key) {
	case "tdate":
		return "2014-12-15T01:01:01.000Z"
	case "tbigint":
		return "1234567890"
	case "tdecimal":
		return "12345.1234"
	case "tint":
		return "1234"
	case "tbool":
		return "true"
	case "tstring":
		return "hello world"
	}

	return "unknown"
}

type TestDescription struct {}

func (d *TestDescription) Available() ([]Source, error) {
	return []Source{
		Source{
			"TestSource",
			"Version1.2",
			"",
		},
	}, nil
}

func (d *TestDescription) FieldNames(string) ([]string, error) {
	return []string{ "tdate", "tbigint", "tint", "tdecimal", "tbool", "tstring" }, nil
}

func (d *TestDescription) Reader(Source) (ValueReader, error) {
	r := &TestReader{}
	return r, nil
}

func TestDiscoversSchema(t *testing.T) {
	spec := tests.Spec(t)

	desc := &TestDescription{}
	s, err := schema(desc)
	if err != nil {
		t.Error(err)
	}
	if s == nil {
		t.Error("s Shouldn't be nil")
	}

	spec.Expect(s[0].SourceName).ToEqual("TestSource")

	spec.Expect(s[0].Fields[0].FieldType).ToEqual("datetime")
	spec.Expect(s[0].Fields[1].FieldType).ToEqual("bigint")
	spec.Expect(s[0].Fields[2].FieldType).ToEqual("int")
	spec.Expect(s[0].Fields[3].FieldType).ToEqual("decimal")
	spec.Expect(s[0].Fields[4].FieldType).ToEqual("boolean")
	spec.Expect(s[0].Fields[5].FieldType).ToEqual("string")

	spec.Expect(s[0].Fields[5].FieldName).ToEqual("tstring")


}