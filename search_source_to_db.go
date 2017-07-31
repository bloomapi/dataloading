package dataloading

import (
	"time"
	"bytes"
	"text/template"
	
	"fmt"
)

type syncInfo struct {
	SearchSource SearchSource
	LastIndexed string
}

var fns = template.FuncMap{
	"eq": func(x, y interface{}) bool {
		return x == y
	},
	"sub": func(y, x int) int {
		return x - y
	},
}

func searchSourceToUpdateQuery(searchSource SearchSource, lastIndexed time.Time) string {
	buf := new(bytes.Buffer)
	t, err := template.New("search.sql.template").Funcs(fns).Parse(searchSql)
	sLastIndexed := lastIndexed.Format(time.RFC3339)
	info := syncInfo{searchSource, sLastIndexed}
	err = t.Execute(buf, info)
	if err != nil {
		fmt.Println(err)
	}
	return buf.String()
}

func searchSourceToDeleteQuery(searchSource SearchSource, lastIndexed time.Time) string {
	pivot := searchSource.Pivot
	idField := searchSource.Id
	fLastIndex := lastIndexed.Format(time.RFC3339)
	return "SELECT " + pivot + "_revisions." + idField + " FROM " + pivot + "_revisions WHERE bloom_action = 'DELETE' AND bloom_updated_at > '" + fLastIndex + "';"
}