Elasticsearch Data Format Plugin
========================

## Overview

Elasticsearch Data Format Plugin provides a feature to allow you to download a response of a search result as several formats other than JSON.
The supported formats are CSV, Excel and JSON(Bulk).

## Version

[Versions in Maven Repository](http://central.maven.org/maven2/org/codelibs/elasticsearch-dataformat/)

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-dataformat/issues "issue").
(Japanese forum is [here](https://github.com/codelibs/codelibs-ja-forum "here").)

## Installation

### For 5.x

    $ $ES_HOME/bin/elasticsearch-plugin install org.codelibs:elasticsearch-dataformat:5.3.0

### For 2.x

    $ $ES_HOME/bin/plugin install org.codelibs/elasticsearch-dataformat/2.4.0

## Supported Output Formats

This plugin allows you to download data as a format you want.
If the query dsl contains "from" parameter, the query is processed as search query.
If not, it's as scan query(all data are stored.).

### CSV

    $ curl -o /tmp/data.csv -XGET "localhost:9200/{index}/{type}/_data?format=csv&source=..."

| Request Parameter | Type    | Description |
|:------------------|:-------:|:------------|
| append.header     | boolean | Append column headers if true |
| fields_name       | string  | choose the fields to dump |
| source            | string  | [Query DSL](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl.html) |
| csv.separator     | string  | Separate character in CSV |
| csv.quote         | string  | Quote character in CSV|
| csv.escape        | string  | Escape character in CSV |
| csv.nullString    | string  | String if a value is null |
| csv.encoding      | string  | Encoding for CSV |

### Excel

    $ curl -o /tmp/data.xls -XGET "localhost:9200/{index}/{type}/_data?format=xls&source=..."

| Request Parameter | Type    | Description |
|:------------------|:-------:|:------------|
| append.header     | boolean | Append column headers if true |
| fields_name       | string  | choose the fields to dump |
| source            | string  | [Query DSL](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl.html) |

### Excel 2007

    $ curl -o /tmp/data.xlsx -XGET "localhost:9200/{index}/{type}/_data?format=xlsx&source=..."

| Request Parameter | Type    | Description |
|:------------------|:-------:|:------------|
| source            | string  | [Query DSL](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl.html) |

### JSON (Elasticsearch Bulk format)

    $ curl -o /tmp/data.json -XGET "localhost:9200/{index}/{type}/_data?format=json&source=..."

| Request Parameter | Type    | Description |
|:------------------|:-------:|:------------|
| source            | string  | [Query DSL](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl.html) |
| bulk.index        | string  | Index name in Bulk file |
| bulk.type         | string  | Type name in Bulk file |

