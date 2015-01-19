package bloomsource

import (
	"time"
	"bytes"
	"text/template"
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
	t, _ := template.New("search.sql.template").Funcs(fns).Parse(searchSql)
	sLastIndexed := lastIndexed.Format(time.RFC3339)
	info := syncInfo{searchSource, sLastIndexed}
	_ = t.Execute(buf, info)
	return buf.String()
}

func searchSourceToDeleteQuery(searchSource SearchSource, lastIndexed time.Time) string {
	pivot := searchSource.Pivot
	idField := searchSource.Id
	fLastIndex := lastIndexed.Format(time.RFC3339)
	return "SELECT " + pivot + "." + idField + " FROM " + pivot + "_revisions WHERE bloom_action = 'DELETE' AND bloom_updated_at > '" + fLastIndex + "';"
}