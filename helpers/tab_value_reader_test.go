package helpers

import (
	"strings"
	"testing"
	"github.com/bloomapi/dataloading/tests"
)

func TestTabValueReaderValue(t *testing.T) {
	spec := tests.Spec(t)

	ioReader := strings.NewReader("1 2 33")

	treader := NewTabReader(ioReader, []TabField{
			TabField{
				Name: "one",
				StartIndex: 1,
				EndIndex: 2,
			},
			TabField{
				Name: "two",
				StartIndex: 3,
				EndIndex: 4,
			},
			TabField{
				Name: "three",
				StartIndex: 5,
				EndIndex: 7,
			},
			TabField{
				Name: "four",
				StartIndex: 8,
				EndIndex: 9,
			},
		})

	row, err := treader.Read()
	spec.Expect(err).ToEqual(nil)

	one, ok := row.Value("one")
	spec.Expect(one).ToEqual("1")
	spec.Expect(ok).ToEqual(true)

	two, ok := row.Value("two")
	spec.Expect(two).ToEqual("2")
	spec.Expect(ok).ToEqual(true)

	three, ok := row.Value("three")
	spec.Expect(three).ToEqual("33")
	spec.Expect(ok).ToEqual(true)

	four, ok := row.Value("four")
	spec.Expect(four).ToEqual("")
	spec.Expect(ok).ToEqual(true)

	five, ok := row.Value("five")
	spec.Expect(five).ToEqual("")
	spec.Expect(ok).ToEqual(false)
}