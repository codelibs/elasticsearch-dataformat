Elasticsearch Data Format Plugin
========================

## Overview

Elasticsearch Data Format Plugin provides a feature to allow you to download a response of a search result as several formats other than JSON.
The supported formats are CSV, Excel, JSON(Bulk) and JSON(Object List).

## Version

[Versions in Maven Repository](https://repo1.maven.org/maven2/org/codelibs/elasticsearch-dataformat/)

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-dataformat/issues "issue").

## Installation

    $ $ES_HOME/bin/elasticsearch-plugin install org.codelibs:elasticsearch-dataformat:7.6.0

## Supported Output Formats

This plugin allows you to download data as a format you want.
By default, the 100 first hits are returned.
You can customize hits returned with `from` and `size` query parameters.
If you want to download all data, use `scroll=1m` query parameter.

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

### JSON (Object List format)

    $ curl -o /tmp/data.json -XGET "localhost:9200/{index}/{type}/_data?format=jsonlist&source=..."

| Request Parameter |  Type  | Description                                                  |
| :---------------- | :----: | :----------------------------------------------------------- |
| source            | string | [Query DSL](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl.html) |

