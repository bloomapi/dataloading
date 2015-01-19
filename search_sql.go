package bloomsource

var searchSql = `
SELECT row_to_json(root), {{.SearchSource.SearchId}} FROM
(SELECT
{{range $i, $e := .SearchSource.Select}}{{$e}}{{if len $.SearchSource.Select | sub 1 | eq $i | not}},{{end}}{{end}}
{{range $i, $e := .SearchSource.Relationships}}
,
(
	SELECT 
	{{if eq .Type "to_many"}}
	  array_to_json(array_agg(row_to_json({{.Name}})))
	{{end}}
	{{if eq .Type "to_one"}}
	  row_to_json({{.Name}})
	{{end}}
  (SELECT 
  {{range $y, $z := .SearchSource.Select}}{{$z}}{{if len $.SearchSource.Select | sub 1 | eq $y | not}},{{end}}{{end}}
  FROM
  {{.Include}}
  WHERE {{$.SearchSource.Pivot}}.{{.SourceId}} = {{$.Include}}.{{.DestId}})
) AS {{.Name}}
{{end}}
FROM {{.SearchSource.Pivot}}
{{range $i, $e := .SearchSource.Joins}}
  LEFT JOIN {{$e.Join}}
  ON {{$e.Join}}.{{$e.DestId}} = {{$.SearchSource.Pivot}}.{{$e.SourceId}}
{{end}}
WHERE
{{.SearchSource.Pivot}}.bloom_created_at > '{{.LastIndexed}}'
{{range $i, $e := .SearchSource.Joins}}
  OR
  {{.Join}}.bloom_created_at > '{{$.LastIndexed}}' OR
  EXISTS(SELECT 1 FROM {{$e.Join}}_revisions WHERE {{$e.Join}}_revisions.bloom_action = 'DELETE' AND {{$e.Join}}_revisions.bloom_updated_at > '{{$.LastIndexed}}' AND {{$e.Join}}_revisions.{{$e.DestId}} = {{$.SearchSource.Pivot}}.{{$e.SourceId}})
{{end}}
{{range $i, $e := .SearchSource.Relationships}}
  OR
  EXISTS(SELECT 1 FROM {{$e.Include}} WHERE {{$e.Include}}.bloom_created_at > '{{$.LastIndexed}}' AND {{$e.Join}}_revisions.{{$e.DestId}} = {{$.Pivot}}.{{$e.SourceId}}) OR
  EXISTS(SELECT 1 FROM {{$e.Include}}_revisions WHERE {{$e.Include}}_revisions.bloom_action = 'DELETE' AND {{$e.Include}}_revisions.bloom_updated_at > '{{$.LastIndexed}}' AND {{$e.Include}}_revisions.{{$e.DestId}} = {{$.SearchSource.Pivot}}.{{$e.SourceId}})
{{end}}
) AS root
`