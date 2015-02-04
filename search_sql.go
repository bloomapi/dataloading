package bloomsource

var searchSql = `
SELECT row_to_json(root), root.{{.SearchSource.SearchId}} FROM
(SELECT
{{range $i, $e := .SearchSource.Select}}{{$e}}{{if len $.SearchSource.Select | sub 1 | eq $i | not}},{{end}}{{end}}
{{range $i, $e := .SearchSource.Relationships}}
,
(
	SELECT 
	{{if eq .Type "to_many"}}
	  array_to_json(array_agg(row_to_json({{.Name}}_table))) AS {{.Name}}
	{{end}}
	{{if eq .Type "to_one"}}
	  row_to_json({{.Name}}_table) AS {{.Name}}
	{{end}}
  FROM
  (SELECT 
  {{range $y, $z := .Select}}{{$z}}{{if len $e.Select | sub 1 | eq $y | not}},{{end}}{{end}}
  FROM
  {{.Include}} two
  {{if and $e.Using (ne $e.Using.Table "")}}
    JOIN {{$e.Using.Table}}
    ON {{$e.Using.Table}}.{{$e.Using.DestId}} = two.{{$e.DestId}}
    WHERE {{$.SearchSource.Pivot}}.{{$e.SourceId}} = {{$e.Using.Table}}.{{$e.Using.SourceId}}
  {{else}}
    WHERE {{$.SearchSource.Pivot}}.{{.SourceId}} = two.{{.DestId}}
  {{end}}
  ) AS {{.Name}}_table)
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
  EXISTS(SELECT 1 FROM {{.Join}}_revisions WHERE {{.Join}}_revisions.bloom_action = 'DELETE' AND {{.Join}}_revisions.bloom_updated_at > '{{$.LastIndexed}}' AND {{.Join}}_revisions.{{.DestId}} = {{$.SearchSource.Pivot}}.{{.SourceId}})
{{end}}
{{range $i, $e := .SearchSource.Relationships}}
  OR
  EXISTS(SELECT 1 FROM {{$e.Include}} two WHERE two.bloom_created_at > '{{$.LastIndexed}}' AND two.{{$e.DestId}} = {{$.SearchSource.Pivot}}.{{$e.SourceId}}) OR
  EXISTS(SELECT 1 FROM {{$e.Include}}_revisions WHERE {{$e.Include}}_revisions.bloom_action = 'DELETE' AND {{$e.Include}}_revisions.bloom_updated_at > '{{$.LastIndexed}}' AND {{$e.Include}}_revisions.{{$e.DestId}} = {{$.SearchSource.Pivot}}.{{$e.SourceId}})
{{end}}
) AS root
WHERE {{.SearchSource.SearchId}} IS NOT NULL
`