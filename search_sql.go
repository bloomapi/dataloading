package bloomsource

var searchSql = `
SELECT row_to_json(root), root.{{.SearchSource.SearchId}} FROM
(SELECT
{{range $i, $e := .SearchSource.SelectTypes}}
{{if eq $e.Type "timestamp without time zone"}}
to_char({{$e.Name}}, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as {{$e.Name}}
{{else}}
{{$e.Name}}
{{end}}{{if len $.SearchSource.SelectTypes | sub 1 | eq $i | not}},{{end}}{{end}}
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
  {{range $y, $z := .SelectTypes}}
  {{if eq $z.Type "timestamp without time zone"}}
  to_char({{$z.Name}}, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as {{$z.Name}}
  {{else}}
  {{$z.Name}}
  {{end}}
  {{if len $e.SelectTypes | sub 1 | eq $y | not}},{{end}}{{end}}
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