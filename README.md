Elasticsearch Data Format Plugin
========================

## Overview

Elasticsearch Data Format Plugin provides a feature to allow you to download a response of a search result as several formats other than JSON.
The supported formats are CSV, Excel and JSON(Bulk).

## Version

| River Web | elasticsearch |
|:---------:|:-------------:|
| master    | 1.0.0.X       |
| 0.2.0     | 1.0.0.RC2     |
| 0.1.0     | 0.90.7        |

## Installation

    $ $ES_HOME/bin/plugin --install org.codelibs/elasticsearch-dataformat/0.2.0

## Supported Output Formats

This plugin allows you to download data as a format you want.
Parameters for a query are the same as a scan query.

### CSV

    $ curl -o /tmp/data.csv -XGET "localhost:9200/{index}/{type}/_data?format=csv&source=..."

| Request Parameter | Type    | Description |
|:------------------|:-------:|:------------|
| fl                | string  | Field names you want to contain in CSV |
| source            | string  | [Query DSL](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl.html) |
| csv.separator     | string  | Separate character in CSV |
| csv.quote         | string  | Quote character in CSV|
| csv.escape        | string  | Escape character in CSV |
| csv.nullString    | string  | String if a value is null |
| append.header     | boolean | Append column headers if true |
| csv.encoding      | string  | Encoding for CSV |

### Excel

    $ curl -o /tmp/data.xls -XGET "localhost:9200/{index}/{type}/_data?format=xls&source=..."

| Request Parameter | Type    | Description |
|:------------------|:-------:|:------------|
| source            | string  | [Query DSL](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl.html) |

### JSON (Elasticsearch Bulk format)

    $ curl -o /tmp/data.json -XGET "localhost:9200/{index}/{type}/_data?format=json&source=..."

| Request Parameter | Type    | Description |
|:------------------|:-------:|:------------|
| source            | string  | [Query DSL](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl.html) |

